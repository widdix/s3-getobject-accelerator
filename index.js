const {PassThrough} = require('node:stream');
const {EventEmitter} = require('node:events');
const {createWriteStream} = require('node:fs');
const querystring = require('node:querystring');
const dns = require('node:dns');
const https = require('node:https');
const http = require('node:http');
const aws4 = require('aws4');
const {parseString} = require('xml2js');

const EVENT_NAME_OBJECT_DOWNLOADING  = 'object:downloading';
const EVENT_NAME_PART_DOWNLOADING = 'part:downloading';
const EVENT_NAME_PART_DOWNLOADED = 'part:downloaded';
const EVENT_NAME_PART_WRITING = 'part:writing';
const EVENT_NAME_PART_DONE = 'part:done';

const EVENT_NAMES = [
  EVENT_NAME_PART_DOWNLOADING,
  EVENT_NAME_PART_DOWNLOADED,
  EVENT_NAME_PART_WRITING,
  EVENT_NAME_PART_DONE
];
exports.EVENT_NAMES = EVENT_NAMES;

const RETRIABLE_NETWORK_ERROR_CODES = ['ECONNRESET', 'ENOTFOUND', 'ESOCKETTIMEDOUT', 'ETIMEDOUT', 'ECONNREFUSED', 'EHOSTUNREACH', 'EPIPE', 'EAI_AGAIN', 'EBUSY'];

const AWS_CREDENTIALS_MAX_AGE_IN_MILLISECONDS = 4*60*1000; // From AWS docs: We make new credentials available at least five minutes before the expiration of the old credentials.
const DNS_RECORD_MAX_AGE_IN_MILLISECONDS = 10*1000;
const IMDS_TOKEN_TTL_IN_SECONDS = 60*10;
const IMDS_TOKEN_MAX_AGE_IN_MILLISECONDS = (IMDS_TOKEN_TTL_IN_SECONDS-60)*1000;
const MAX_RETRY_DELAY_IN_SECONDS = 20;

let imdsTokenCache = undefined;
let imdsRegionCache = undefined;
let imdsCredentialsCache = undefined;
let dnsCache = {};

exports.clearCache = () => {
  imdsTokenCache = undefined;
  imdsRegionCache = undefined;
  imdsCredentialsCache = undefined;
  dnsCache = {};
};

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

function request(nodemodule, options, body, cb) {
  options.lookup = getDnsCache;
  const req = nodemodule.request(options, (res) => {
    let size = ('content-length' in res.headers) ? parseInt(res.headers['content-length'], 10) : 0;
    const bodyChunks = ('content-length' in res.headers) ? null : [];
    const bodyBuffer = ('content-length' in res.headers) ? Buffer.allocUnsafe(size) : null;
    let bodyBufferOffset = 0;
    res.on('data', chunk => {
      if (bodyChunks !== null) {
        bodyChunks.push(chunk);
        size += chunk.length;
      } else {
        chunk.copy(bodyBuffer, bodyBufferOffset);
        bodyBufferOffset += chunk.length;
      }
    });
    res.on('end', () => {
      if (bodyChunks !== null) {
        cb(null, res, Buffer.concat(bodyChunks));
      } else {
        cb(null, res, bodyBuffer);
      }
    });
  });
  req.once('error', (err) => {
    cb(err);
  });
  req.once('timeout', () => {
    req.abort();
  });
  if (Buffer.isBuffer(body)) {
    req.write(body);
  }
  req.end();
}
exports.request = request;

function retryrequest(nodemodule, requestOptions, body, retryOptions, cb) {
  let attempt = 1;
  const retry = (err) => {
    attempt++;
    if (attempt <= retryOptions.maxAttempts) {
      const maxRetryDelayInSeconds = ('maxRetryDelayInSeconds' in retryOptions) ? retryOptions.maxRetryDelayInSeconds : MAX_RETRY_DELAY_IN_SECONDS;
      const delayInMilliseconds = Math.min(Math.random() * Math.pow(2, attempt-1), maxRetryDelayInSeconds) * 1000;
      setTimeout(() => {
        if (requestOptions.signal) {
          if (requestOptions.signal.aborted === true) {
            cb(requestOptions.signal.reason);
          } else {
            req();
          }
        } else {
          req();
        }
      }, delayInMilliseconds);
    } else {
      cb(err);
    }
  };
  const req = () => {
    request(nodemodule, requestOptions, body, (err, res, body) => {
      if (err) {
        if (RETRIABLE_NETWORK_ERROR_CODES.includes(err.code)) {
          retry(err);
        } else {
          cb(err);
        }
      } else {
        if (res.statusCode == 429 || (res.statusCode >= 500 && res.statusCode <= 599)) {
          if (res.headers['content-type'] === 'application/xml') {
            const err = new Error(`status code: ${res.statusCode}\n${body.toString('utf8')}`);
            err.statusCode = res.statusCode;
            retry(err);
          } else {
            const err = new Error(`status code: ${res.statusCode}, content-type: ${res.headers['content-type']}`);
            err.statusCode = res.statusCode;
            retry(err);
          }
        } else {
          cb(null, res, body);
        }
      }
    });
  };
  req();
}
exports.retryrequest = retryrequest;

function imdsRequest(method, path, headers, timeout, cb) {
  const options = {
    hostname: '169.254.169.254',
    method,
    path,
    headers,
    timeout
  };
  retryrequest(http, options, undefined, {maxAttempts: 3}, (err, res, body) => {
    if (err) {
      cb(err);
    } else {
      if (res.statusCode === 200) {
        cb(null, body.toString('utf8'));
      } else {
        const err = new Error(`unexpected IMDS status code: ${res.statusCode}.\n${body.toString('utf8')}`);
        err.statusCode = res.statusCode;
        cb(err);
      }
    }
  });
}

function refreshImdsToken(timeout) {
  imdsTokenCache = new Promise((resolve, reject) => {
    imdsRequest('PUT', '/latest/api/token', {'X-aws-ec2-metadata-token-ttl-seconds': `${IMDS_TOKEN_TTL_IN_SECONDS}`}, timeout, (err, token) => {
      if (err) {
        reject(err);
      } else {
        resolve({
          token,
          cachedAt: Date.now()
        });
      }
    });
  });
  return imdsTokenCache;
}

function getImdsToken(timeout, cb) {
  if (imdsTokenCache === undefined) {
    refreshImdsToken(timeout).then(({token}) => cb(null, token)).catch(cb);
  } else {
    imdsTokenCache.then(({token, cachedAt}) => {
      if ((Date.now()-cachedAt) > IMDS_TOKEN_MAX_AGE_IN_MILLISECONDS) {
        imdsTokenCache = undefined;
        getImdsToken(timeout, cb);
      } else {
        cb(null, token);
      }
    }).catch(cb);
  }
}

function imds(path, timeout, cb) {
  getImdsToken(timeout, (err, token) => {
    if (err) {
      cb(err);
    } else {
      imdsRequest('GET', path, {'X-aws-ec2-metadata-token': token}, timeout, cb);
    }
  });
}
exports.imds = imds;

function refreshAwsRegion(timeout) {
  imdsRegionCache = new Promise((resolve, reject) => {
    if ('AWS_REGION' in process.env) {
      resolve(process.env.AWS_REGION);
    } else {
      imds('/latest/dynamic/instance-identity/document', timeout, (err, body) => {
        if (err) {
          reject(err);
        } else {
          const {region} = JSON.parse(body);
          resolve(region);
        }
      });
    }
  });
  return imdsRegionCache;
}

function getAwsRegion(timeout, cb) {
  if (imdsRegionCache === undefined) {
    refreshAwsRegion(timeout).then(region => cb(null, region)).catch(cb);
  } else {
    imdsRegionCache.then(region => cb(null, region)).catch(cb);
  }
}

function refreshAwsCredentials(timeout) {
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
      imds('/latest/meta-data/iam/security-credentials/', timeout, (err, body) => {
        if (err) {
          reject(err);
        } else {
          const roleName = body.trim();
          imds(`/latest/meta-data/iam/security-credentials/${roleName}`, timeout, (err, body) => {
            if (err) {
              reject(err);
            } else {
              const json = JSON.parse(body);
              resolve({
                accessKeyId: json.AccessKeyId,
                secretAccessKey: json.SecretAccessKey,
                sessionToken: json.Token,
                cachedAt: Date.now()
              });
            }
          });
        }
      });
    }
  });
  return imdsCredentialsCache;
}

function getAwsCredentials({timeout, v2AwsSdkCredentials}, cb) {
  if (!(v2AwsSdkCredentials === undefined || v2AwsSdkCredentials === null)) {
    v2AwsSdkCredentials.get((err) => {
      if (err) {
        cb(err);
      } else {
        const credentials = {
          accessKeyId: v2AwsSdkCredentials.accessKeyId,
          secretAccessKey: v2AwsSdkCredentials.secretAccessKey
        };
        if (v2AwsSdkCredentials.sessionToken) {
          credentials.sessionToken = v2AwsSdkCredentials.sessionToken;
        }
        cb(null, credentials);
      }
    });
  } else if (imdsCredentialsCache === undefined) {
    refreshAwsCredentials(timeout).then(credentials => cb(null, credentials)).catch(cb);
  } else {
    imdsCredentialsCache.then(credentials => {
      if ((Date.now()-credentials.cachedAt) > AWS_CREDENTIALS_MAX_AGE_IN_MILLISECONDS) {
        imdsCredentialsCache = undefined;
        getAwsCredentials({timeout, v2AwsSdkCredentials}, cb);
      } else {
        cb(null, credentials);
      }
    }).catch(cb);
  }
}

function getRecord() {
  const record = this.records[this.ringIndex++];
  if (this.ringIndex >= this.records.length) {
    this.ringIndex = 0;
  }
  return record;
}

function refreshDnsCache(hostname, options) { // eslint-disable-line no-unused-vars
  // A = IPv4
  dnsCache[hostname] = new Promise((resolve, reject) => {
    dns.resolve(hostname, 'A', (err, records) => {
      if (err) {
        reject(err);
      } else {
        if (records.length === 0) {
          reject(new Error('no DNS records returned'));
        } else {
          resolve({
            ringIndex: 0,
            records,
            cachedAt: Date.now(),
            getRecord
          });
        }
      }
    });
  });
  return dnsCache[hostname];
}

// signature is defined by Node.js (https://nodejs.org/api/dns.html#dnslookuphostname-options-callback)
function getDnsCache(hostname, options, cb) {
  // 4 = IPv4
  const cache = dnsCache[hostname];
  if (cache === undefined) {
    refreshDnsCache(hostname, options).then(dns => cb(null, dns.getRecord(), 4)).catch(cb);
  } else {
    cache.then(dns => {
      if ((Date.now()-dns.cachedAt) > DNS_RECORD_MAX_AGE_IN_MILLISECONDS) {
        delete dnsCache[hostname];
        getDnsCache(hostname, options, cb);
      } else {
        cb(null, dns.getRecord(), 4);
      }
    }).catch(cb);
  }
}

function getHostname(region) {
  // TODO Switch to virtual-hosted–style requests as soon as bucket names with a dot are supported (https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html)
  return `s3.${region}.amazonaws.com`;
}

function mapTimeout(connectionTimeoutInMilliseconds) {
  if (connectionTimeoutInMilliseconds === undefined || connectionTimeoutInMilliseconds === null) {
    return 3000;
  }
  if (connectionTimeoutInMilliseconds < 0) {
    throw new Error('connectionTimeoutInMilliseconds >= 0');
  }
  if (connectionTimeoutInMilliseconds === 0) {
    return undefined;
  }
  return connectionTimeoutInMilliseconds;
}

function mapPartSizeInBytes(partSizeInMegabytes) {
  if (partSizeInMegabytes === undefined || partSizeInMegabytes === null) {
    return null;
  }
  if (partSizeInMegabytes <= 0) {
    throw new Error('partSizeInMegabytes > 0');
  }
  return partSizeInMegabytes*1000000;
}

function escapeKey(string) { // source https://github.com/aws/aws-sdk-js/blob/64eb16f8e9a835e41cf47d0efd7bf43dcde9dcb9/lib/util.js#L39-L49
  return encodeURIComponent(string)
    .replace(/[^A-Za-z0-9_.~\-%]+/g, escape)
    .replace(/[*]/g, function(ch) { // AWS percent-encodes some extra non-standard characters in a URI
      return '%' + ch.charCodeAt(0).toString(16).toUpperCase();
    });
}

function getObject({Bucket, Key, VersionId, PartNumber, Range}, {timeout, v2AwsSdkCredentials}, cb) {
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
  getAwsRegion(timeout, (err, region) => {
    if (err) {
      cb(err);
    } else {
      getAwsCredentials({timeout, v2AwsSdkCredentials}, (err, credentials) => {
        if (err) {
          cb(err);
        } else {
          const options = aws4.sign({
            hostname: getHostname(region),
            method: 'GET',
            path: `/${Bucket}/${escapeKey(Key)}?${querystring.stringify(qs)}`,
            headers,
            service: 's3',
            region,
            signal: ac.signal,
            timeout
          }, credentials);
          retryrequest(https, options, undefined, {maxAttempts: 5}, (err, res, body) => {
            if (err) {
              cb(err);
            } else {
              if (res.statusCode === 200 && body.length === 0) {
                const data = {
                  Body: body,
                  ContentLength: body.length
                };
                cb(null, data);
              } else if (res.statusCode === 206) {
                const data = {
                  Body: body,
                  ContentLength: body.length
                };
                if ('x-amz-mp-parts-count' in res.headers) {
                  data.PartsCount = parseInt(res.headers['x-amz-mp-parts-count'], 10);
                }
                if ('content-range' in res.headers) {
                  data.ContentRange = res.headers['content-range'];
                }
                cb(null, data);
              } else {
                if (res.headers['content-type'] === 'application/xml') {
                  parseString(body.toString('utf8'), {explicitArray: false}, function (err, result) {
                    if (err) {
                      cb(err);
                    } else {
                      if (result.Error && result.Error.Code && result.Error.Message) {
                        const e = new Error(`${result.Error.Code}: ${result.Error.Message}`);
                        e.code = result.Error.Code;
                        e.statusCode = res.statusCode;
                        cb(e);
                      } else {
                        const err = new Error(`unexpected S3 XML response (${res.statusCode}):\n${body.toString('utf8')}`);
                        err.statusCode = res.statusCode;
                        cb(err);
                      }
                    }
                  });
                } else {
                  const err = new Error(`unexpected S3 response (${res.statusCode}, ${res.headers['content-type']})`);
                  err.statusCode = res.statusCode;
                  cb(err);
                }
              }
            }
          });
        }
      });
    }
  });
  return ac;
}

exports.download = ({bucket, key, version}, {partSizeInMegabytes, concurrency, connectionTimeoutInMilliseconds, v2AwsSdkCredentials}) => {
  if (concurrency < 1) {
    throw new Error('concurrency > 0');
  }
  if (!(v2AwsSdkCredentials === undefined || v2AwsSdkCredentials === null)) {
    if (typeof v2AwsSdkCredentials.get !== 'function') {
      throw new Error('invalid v2AwsSdkCredentials');
    }
  }
  const timeout = mapTimeout(connectionTimeoutInMilliseconds);

  const emitter = new EventEmitter();
  const partSizeInBytes = mapPartSizeInBytes(partSizeInMegabytes);
  let stream = null;

  let started = false;
  let partsToDownload = -1;
  let bytesToDownload = -1;
  let nextPartNo = -1; // starts at 1 (not at 0)
  let lastWrittenPartNo = -1;
  const partsWaitingForWrite = {};
  const partsDownloading = {};
  let aborted = false;

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
      emitter.emit(EVENT_NAME_PART_WRITING, {partNo});
      write(chunk, () => {
        lastWrittenPartNo = partNo;
        if (lastWrittenPartNo === partsToDownload) {
          stream.end(cb);
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
    const req = getObject(params, {timeout, v2AwsSdkCredentials}, (err, data) => {
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
      emitter.emit(EVENT_NAME_PART_DOWNLOADING, {partNo});
      downloadPart(partNo, (err, data) => {
        if (err) {
          abortDownloads(err);
        } else {
          emitter.emit(EVENT_NAME_PART_DOWNLOADED, {partNo});
          writePart(partNo, data.Body, () => {
            emitter.emit(EVENT_NAME_PART_DONE, {partNo});
            process.nextTick(downloadNextPart);
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

  let metadataCache = null;

  function meta(cb) {
    if (metadataCache === null) {
      metadataCache = new Promise((resolve, reject) => {
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
        partsDownloading[1] = getObject(params, {timeout, v2AwsSdkCredentials}, (err, data) => {
          delete partsDownloading[1];
          if (err) {
            if (err.code === 'InvalidRange') {
              resolve({metadata: {lengthInBytes: 0}, body: Buffer.alloc(0)});
            } else {
              reject(err);
            }
          } else {
            if (data.ContentLength === 0) {
              resolve({metadata: {lengthInBytes: 0}, body: Buffer.alloc(0)});
            } else {
              const contentRange = parseContentRange(data.ContentRange);
              if (contentRange === undefined) {
                reject(new Error(`unexpected S3 content range: ${data.ContentRange}`));
              } else {
                const metadata = {
                  lengthInBytes: contentRange.length
                };
                if ('PartsCount' in data) {
                  metadata.parts = data.PartsCount;
                }
                resolve({metadata, body: data.Body});
              }
            }
          }
        });
      });
    }
    metadataCache.then(({metadata, body}) => {
      cb(null, metadata, body);
    }).catch(cb);
  }

  function start() {
    emitter.emit(EVENT_NAME_PART_DOWNLOADING, {partNo: 1});
    meta((err, metadata, body) => {
      if (err) {
        stream.destroy(err);
      } else {
        emitter.emit(EVENT_NAME_OBJECT_DOWNLOADING, metadata);
        if (partSizeInBytes === null) {
          emitter.emit(EVENT_NAME_PART_DOWNLOADED, {partNo: 1});
          if ('parts' in metadata && metadata.parts > 1) {
            emitter.emit(EVENT_NAME_PART_WRITING, {partNo: 1});
            write(body, () => {
              emitter.emit(EVENT_NAME_PART_DONE, {partNo: 1});
              lastWrittenPartNo = 1;
              nextPartNo = 2;
              partsToDownload = metadata.parts;
              startDownloadingParts();
            });
          } else {
            emitter.emit(EVENT_NAME_PART_WRITING, {partNo: 1});
            stream.end(body, () => {
              emitter.emit(EVENT_NAME_PART_DONE, {partNo: 1});
            });
          }
        } else {
          emitter.emit(EVENT_NAME_PART_DOWNLOADED, {partNo: 1});
          bytesToDownload = metadata.lengthInBytes;
          if (bytesToDownload <= partSizeInBytes) {
            emitter.emit(EVENT_NAME_PART_WRITING, {partNo: 1});
            stream.end(body, () => {
              emitter.emit(EVENT_NAME_PART_DONE, {partNo: 1});
            });
          } else {
            emitter.emit(EVENT_NAME_PART_WRITING, {partNo: 1});
            write(body, () => {
              emitter.emit(EVENT_NAME_PART_DONE, {partNo: 1});
              lastWrittenPartNo = 1;
              nextPartNo = 2;
              partsToDownload = Math.ceil(bytesToDownload/partSizeInBytes);
              startDownloadingParts();
            });
          }
        }
      }
    });
  }

  return {
    meta: (cb) => {
      meta((err, metadata) => {
        if (err) {
          cb(err);
        } else {
          cb(null, metadata);
        }
      });
    },
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
      let calledback = false;
      stream.once('close', () => {
        if (calledback === false) {
          calledback = true;
          cb();
        }
      });
      stream.once('error', (err) => {
        if (calledback === false) {
          calledback = true;
          cb(err);
        }
      });
    },
    abort: (err) => {
      if (err === undefined || err === null) {
        err = new Error('aborted');
      }
      abortDownloads(err);
    },
    partsDownloading: () => Object.keys(partsDownloading).length,
    addListener: (eventName, listener) => emitter.addListener(eventName, listener),
    off: (eventName, listener) => emitter.off(eventName, listener),
    on: (eventName, listener) => emitter.on(eventName, listener),
    once: (eventName, listener) => emitter.once(eventName, listener),
    removeListener: (eventName, listener) => emitter.removeListener(eventName, listener)
  };
};
