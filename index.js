const {PassThrough} = require('node:stream');
const {EventEmitter} = require('node:events');
const {createWriteStream} = require('node:fs');
const querystring = require('node:querystring');
const dns = require('node:dns');
const https = require('node:https');
const http = require('node:http');
const aws4 = require('aws4');

function parseContentRange(contentRange) {
  // bytes 0-7999999/1073741824
  if (contentRange.startsWith('bytes ')) {
    const [range, length] = contentRange.substr(6).split('/');
    const [startByte, endByte] = range.split('-');
    return {
      length: parseInt(length, 10),
      startByte: parseInt(startByte, 10),
      endByte: parseInt(endByte, 10)
    };
  } else {
    return undefined;
  }
}

function imdsRequest(method, path, headers, cb) {
  const options = {
    hostname: '169.254.169.254',
    method,
    path,
    headers,
    timeout: 3000
  };
  const req = http.request(options, (res) => {
    const size = ('content-length' in res.headers) ? parseInt(res.headers['content-length'], 10) : 0;
    const body = Buffer.allocUnsafe(size);
    let bodyOffset = 0;
    res.on('data', chunk => {
      chunk.copy(body, bodyOffset);
      bodyOffset += chunk.length;
    });
    if (res.statusCode === 200) {
      res.on('end', () => {
        cb(null, body.toString('utf8'));
      });
    } else {
      res.on('end', () => {
        cb(new Error(`unexpected status code: ${res.statusCode}.\n${body.toString('utf8')}`));
      });
    }
  });
  req.on('error', (err) => {
    cb(err);
  });
  req.on('timeout', () => {
    req.abort();
    cb(new Error('IMDS request timeout'));
  });
  req.end();
}

function imds(path, cb) {
  imdsRequest('PUT', '/latest/api/token', {'X-aws-ec2-metadata-token-ttl-seconds': '60'}, (err, token) => {
    if (err) {
      cb(err);
    } else {
      imdsRequest('GET', path, {'X-aws-ec2-metadata-token': token}, cb);
    }
  });
}

let imdsRegionCache = null;

function refreshAwsRegion() {
  imdsRegionCache = new Promise((resolve, reject) => {
    if ('AWS_REGION' in process.env) {
      resolve(process.env.AWS_REGION);
    } else {
      imds('/latest/dynamic/instance-identity/document', (err, body) => {
        if (err) {
          reject(err);
        } else {
          const {region} = JSON.parse(body);
          resolve(region);
        }
      });
    }
  });
}

function getAwsRegion(cb) {
  if (imdsRegionCache === null) {
    cb(new Error('region cache empty'));
  } else {
    imdsRegionCache.then(region => cb(null, region)).catch(cb);
  }
}

let imdsCredentialsCache = null;

function refreshAwsCredentials() {
  imdsCredentialsCache = new Promise((resolve, reject) => {
    if ('AWS_ACCESS_KEY_ID' in process.env && 'AWS_SECRET_ACCESS_KEY' in process.env) {
      const credentials = {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
      };
      if ('AWS_SESSION_TOKEN' in process.env) {
        credentials.sessionToken = process.env.AWS_SESSION_TOKEN;
      }
      resolve(credentials);
    } else {
      imds('/latest/meta-data/iam/security-credentials/', (err, body) => {
        if (err) {
          reject(err);
        } else {
          const roleName = body.trim();
          imds(`/latest/meta-data/iam/security-credentials/${roleName}`, (err, body) => {
            if (err) {
              reject(err);
            } else {
              const json = JSON.parse(body);
              resolve({
                accessKeyId: json.AccessKeyId,
                secretAccessKey: json.SecretAccessKey,
                sessionToken: json.Token
              });
            }
          });
        }
      });
    }
  });
}

function getAwsCredentials(cb) {
  if (imdsCredentialsCache === null) {
    cb(new Error('credentials cache empty'));
  } else {
    imdsCredentialsCache.then(credentials => cb(null, credentials)).catch(cb);
  }
}

function getHostname(bucket, region) {
  return `${bucket}.s3.${region}.amazonaws.com`;
}

exports.download = ({bucket, key, version}, {partSizeInMegabytes, concurrency, waitForWriteBeforeDownloladingNextPart}) => {
  if (partSizeInMegabytes !== undefined && partSizeInMegabytes !== null && partSizeInMegabytes <= 0) {
    throw new Error('partSizeInMegabytes > 0');
  }
  if (concurrency < 1) {
    throw new Error('concurrency > 0');
  }

  const emitter = new EventEmitter();
  const partSizeInBytes = (partSizeInMegabytes !== undefined && partSizeInMegabytes !== null) ? partSizeInMegabytes*1000000 : null;
  let stream = null;

  let started = false;
  let partsToDownload = -1;
  let bytesToDownload = -1;
  let nextPartNo = -1; // starts at 1 (not at 0)
  let lastWrittenPartNo = -1;
  const partsWaitingForWrite = {};
  const partsDownloading = {};
  let aborted = false;
  let dnsCacheInterval = null;
  let dnsCache = [];
  let dnsCacheRingIndex = 0;

  function populateDnsCache(hostname, cb) {
    dns.resolve(hostname, 'A', (err, records) => { // A = IPv4
      if (err) {
        cb(err);
      } else {
        dnsCache = records;
        emitter.emit('dns:updated', [...dnsCache]);
        dnsCacheRingIndex = 0;
        cb();
      }
    });
  }

  function lookup(hostname, options, cb) {
    if (dnsCache.length === 0) {
      cb(new Error('DNS cache empty'));
    } else {
      const cachedRecord = dnsCache[dnsCacheRingIndex++];
      if (dnsCacheRingIndex >= dnsCache.length) {
        dnsCacheRingIndex = 0;
      }
      cb(null, cachedRecord, 4); // 4 = IPv4
    }
  }

  function escapeKey(string) { // source https://github.com/aws/aws-sdk-js/blob/64eb16f8e9a835e41cf47d0efd7bf43dcde9dcb9/lib/util.js#L39-L49
    return encodeURIComponent(string)
      .replace(/[^A-Za-z0-9_.~\-%]+/g, escape)
      .replace(/[*]/g, function(ch) { // AWS percent-encodes some extra non-standard characters in a URI
        return '%' + ch.charCodeAt(0).toString(16).toUpperCase();
      });
  }

  function getObject({Bucket, Key, VersionId, PartNumber, Range}, cb) {
    const ac = new AbortController();
    const qs = {};
    const headers = {};
    if (VersionId !== undefined && VersionId !== null) {
      qs.versionId = VersionId;
    }
    if (PartNumber !== undefined && PartNumber !== null) {
      qs.partNumber = PartNumber;
    }
    if (Range !== undefined && Range !== null) {
      headers.Range = Range;
    }
    getAwsRegion((err, region) => {
      if (err) {
        cb(err);
      } else {
        getAwsCredentials((err, credentials) => {
          if (err) {
            cb(err);
          } else {
            const options = aws4.sign({
              hostname: getHostname(Bucket, region),
              method: 'GET',
              path: `/${escapeKey(Key)}?${querystring.stringify(qs)}`,
              headers,
              service: 's3',
              region,
              signal: ac.signal,
              lookup,
              timeout: 3000
            }, credentials);
            const req = https.request(options, (res) => {
              const size = ('content-length' in res.headers) ? parseInt(res.headers['content-length'], 10) : 0;
              const body = Buffer.allocUnsafe(size);
              let bodyOffset = 0;
              res.on('data', chunk => {
                chunk.copy(body, bodyOffset);
                bodyOffset += chunk.length;
              });
              if (res.statusCode === 206) {
                res.on('end', () => {
                  const data = {
                    Body: body,
                    ContentLength: size
                  };
                  if ('x-amz-mp-parts-count' in res.headers) {
                    data.PartsCount = parseInt(res.headers['x-amz-mp-parts-count'], 10);
                  }
                  if ('content-range' in res.headers) {
                    data.ContentRange = res.headers['content-range'];
                  }
                  cb(null, data);
                });
              } else {
                res.on('end', () => {
                  cb(new Error(`unexpected status code: ${res.statusCode}.\n${body.toString('utf8')}`));
                });
              }
            });
            req.on('error', (err) => {
              cb(err);
            });
            req.on('timeout', () => {
              req.abort();
              cb(new Error('S3 request timeout'));
            });
            req.end();
          }
        });
      }
    });
    return ac;
  }

  function stop() {
    if (dnsCacheInterval !== null) {
      clearInterval(dnsCacheInterval);
      dnsCacheInterval = null;
    }
  }

  function write(chunk, cb) {
    if (!stream.write(chunk)) {
      stream.once('drain', cb);
    } else {
      process.nextTick(cb);
    }
  }

  function abortDownloads(err) {
    if (aborted === false) {
      aborted = true;
      Object.values(partsDownloading).forEach(req => req.abort());
      stream.destroy(err);
      stop();
    }
  }

  function drainWriteQueue() {
    const nextPartNoToWrite = lastWrittenPartNo+1;
    const part = partsWaitingForWrite[nextPartNoToWrite];
    if (part !== undefined) {
      delete partsWaitingForWrite[nextPartNoToWrite];
      writePart(part.partNo, part.chunk, part.cb);
    }
  }

  function writePart(partNo, chunk, cb) {
    if (lastWrittenPartNo === (partNo-1)) {
      write(chunk, () => {
        lastWrittenPartNo = partNo;
        if (lastWrittenPartNo === partsToDownload) {
          stream.end(cb);
          stop();
        } else {
          process.nextTick(drainWriteQueue);
          cb();
        }
      });
    } else {
      partsWaitingForWrite[partNo] = {partNo, chunk, cb};
    }
  }

  function downloadPart(partNo, cb) {
    const params = {
      Bucket: bucket,
      Key: key,
      VersionId: version
    };
    if (partSizeInBytes === null) {
      params.PartNumber = partNo;
    } else {
      const startByte = (partNo-1)*partSizeInBytes; // inclusive
      const endByte = Math.min(startByte+partSizeInBytes-1, bytesToDownload-1); // inclusive
      params.Range = `bytes=${startByte}-${endByte}`;
    }
    const req = getObject(params, (err, data) => {
      delete partsDownloading[partNo];
      if (err) {
        cb(err);
      } else {
        cb(null, data);
      }
    });
    partsDownloading[partNo] = req;
  }

  function downloadNextPart() {
    if (nextPartNo <= partsToDownload) {
      const partNo = nextPartNo++;
      emitter.emit('part:downloading', {partNo});
      downloadPart(partNo, (err, data) => {
        if (err) {
          abortDownloads(err);
        } else {
          emitter.emit('part:downloaded', {partNo});
          if (waitForWriteBeforeDownloladingNextPart !== true) {
            process.nextTick(downloadNextPart);
          }
          writePart(partNo, data.Body, () => {
            emitter.emit('part:done', {partNo});
            if (waitForWriteBeforeDownloladingNextPart === true) {
              process.nextTick(downloadNextPart);
            }
          });
        }
      });
    }
  }

  function startDownloadingParts() {
    for (let i = 0; i < concurrency; i++) {
      downloadNextPart();
    }
  }

  function start() {
    refreshAwsCredentials();
    refreshAwsRegion();
    getAwsRegion((err, region) => {
      if (err) {
        stream.destroy(err);
        stop();
      } else {
        dnsCacheInterval = setInterval(() => {
          populateDnsCache(getHostname(bucket, region), () => {});
        }, 5000);
        populateDnsCache(getHostname(bucket, region), (err) => {
          if (err) {
            stream.destroy(err);
            stop();
          } else {
            emitter.emit('part:downloading', {partNo: 1});
            const params = {
              Bucket: bucket,
              Key: key,
              VersionId: version
            };
            if (partSizeInBytes === null) {
              params.PartNumber = 1;
            } else {
              const endByte = partSizeInBytes-1; // inclusive
              params.Range = `bytes=0-${endByte}`;
            }
            getObject(params, (err, data) => {
              if (err) {
                stream.destroy(err);
                stop();
              } else {
                if (partSizeInBytes === null) {
                  emitter.emit('part:downloaded', {partNo: 1});
                  if ('PartsCount' in data && data.PartsCount > 1) {
                    write(data.Body, () => {
                      emitter.emit('part:done', {partNo: 1});
                      lastWrittenPartNo = 1;
                      nextPartNo = 2;
                      partsToDownload = data.PartsCount;
                      startDownloadingParts();
                    });
                  } else {
                    stream.end(data.Body, () => {
                      emitter.emit('part:done', {partNo: 1});
                      stop();
                    });
                  }
                } else {
                  const contentRange = parseContentRange(data.ContentRange);
                  if (contentRange === undefined) {
                    stream.destroy(new Error('unexpected content range'));
                    stop();
                  } else {
                    emitter.emit('part:downloaded', {partNo: 1});
                    bytesToDownload = contentRange.length;
                    if (bytesToDownload <= partSizeInBytes) {
                      stream.end(data.Body, () => {
                        emitter.emit('part:done', {partNo: 1});
                        stop();
                      });
                    } else {
                      write(data.Body, () => {
                        emitter.emit('part:done', {partNo: 1});
                        lastWrittenPartNo = 1;
                        nextPartNo = 2;
                        partsToDownload = Math.ceil(bytesToDownload/partSizeInBytes);
                        startDownloadingParts();
                      });
                    }
                  }
                }
              }
            });
          }
        });
      }
    });
  }

  return {
    readStream: () => {
      if (started === false)  {
        stream = new PassThrough();
        start();
      }
      return stream;
    },
    file: (path, cb) => {
      if (started === false)  {
        stream = createWriteStream(path);
        start();
      }
      stream.once('end', () => cb());
      stream.once('error', cb);
    },
    partsDownloading: () => Object.keys(partsDownloading).length,
    addListener: (eventName, listener) => emitter.addListener(eventName, listener),
    off: (eventName, listener) => emitter.off(eventName, listener),
    on: (eventName, listener) => emitter.on(eventName, listener),
    once: (eventName, listener) => emitter.once(eventName, listener),
    removeListener: (eventName, listener) => emitter.removeListener(eventName, listener)
  };
};
