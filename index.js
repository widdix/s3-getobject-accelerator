const {PassThrough} = require('node:stream');
const {EventEmitter} = require('node:events');
const {createWriteStream} = require('node:fs');
const querystring = require('node:querystring');
const {Resolver} = require('node:dns');
const https = require('node:https');
const http = require('node:http');
const aws4 = require('aws4');
const {parseString} = require('xml2js');
const {LRUCache} = require('lru-cache');

const EVENT_NAME_REQUEST_NAME_RESOLVING = 'request:name-resolving';
const EVENT_NAME_REQUEST_NAME_RESOLVED = 'request:name-resolved';
const EVENT_NAME_REQUEST_CONNECTING = 'request:connecting';
const EVENT_NAME_REQUEST_CONNECTED = 'request:connected';
const EVENT_NAME_REQUEST_BODY_WRITING = 'request:body-writing';
const EVENT_NAME_REQUEST_BODY_WRITTEN = 'request:body-written';
const EVENT_NAME_REQUEST_BODY_READING = 'request:body-reading';
const EVENT_NAME_REQUEST_BODY_READING_PROGRESS = 'request:body-reading-progress';
const EVENT_NAME_REQUEST_BODY_READ = 'request:body-read';
const EVENT_NAME_REQUEST_RETRYING = 'request:retrying';
const EVENT_NAME_OBJECT_DOWNLOADING  = 'object:downloading';
const EVENT_NAME_PART_DOWNLOADING = 'part:downloading';
const EVENT_NAME_PART_DOWNLOADED = 'part:downloaded';
const EVENT_NAME_PART_WRITING = 'part:writing';
const EVENT_NAME_PART_DONE = 'part:done';

const EVENT_NAMES = [
  EVENT_NAME_REQUEST_NAME_RESOLVING,
  EVENT_NAME_REQUEST_NAME_RESOLVED,
  EVENT_NAME_REQUEST_CONNECTING,
  EVENT_NAME_REQUEST_CONNECTED,
  EVENT_NAME_REQUEST_BODY_WRITING,
  EVENT_NAME_REQUEST_BODY_WRITTEN,
  EVENT_NAME_REQUEST_BODY_READING,
  EVENT_NAME_REQUEST_BODY_READING_PROGRESS,
  EVENT_NAME_REQUEST_BODY_READ,
  EVENT_NAME_REQUEST_RETRYING,
  EVENT_NAME_OBJECT_DOWNLOADING,
  EVENT_NAME_PART_DOWNLOADING,
  EVENT_NAME_PART_DOWNLOADED,
  EVENT_NAME_PART_WRITING,
  EVENT_NAME_PART_DONE
];
exports.EVENT_NAMES = EVENT_NAMES;

const READ_STATS_SAMPLE_RATE_IN_SECONDS = 5;

class RequestTimeoutError extends Error {
  constructor(message) {
    super(message);
    this.name = 'RequestTimeoutError';
  }
}
exports.RequestTimeoutError = RequestTimeoutError;

class ConnectionTimeoutError extends Error {
  constructor(message) {
    super(message);
    this.name = 'ConnectionTimeoutError';
  }
}
exports.ConnectionTimeoutError = ConnectionTimeoutError;

class ReadTimeoutError extends Error {
  constructor(message) {
    super(message);
    this.name = 'ReadTimeoutError';
  }
}
exports.ReadTimeoutError = ReadTimeoutError;

class DataTimeoutError extends Error {
  constructor(message) {
    super(message);
    this.name = 'DataTimeoutError';
  }
}
exports.DataTimeoutError = DataTimeoutError;

class WriteTimeoutError extends Error {
  constructor(message) {
    super(message);
    this.name = 'WriteTimeoutError';
  }
}
exports.WriteTimeoutError = WriteTimeoutError;

const RETRIABLE_NETWORK_ERROR_CODES = ['ECONNRESET', 'ENOTFOUND', 'ESOCKETTIMEDOUT', 'ETIMEDOUT', 'ECONNREFUSED', 'EHOSTUNREACH', 'EPIPE', 'EAI_AGAIN', 'EBUSY'];
const RETRIABLE_ERROR_NAMES = ['RequestTimeoutError', 'ConnectionTimeoutError', 'ReadTimeoutError', 'DataTimeoutError', 'WriteTimeoutError'];

const AWS_CREDENTIALS_MAX_AGE_IN_MILLISECONDS = 4*60*1000; // From AWS docs: We make new credentials available at least five minutes before the expiration of the old credentials.
const DNS_RECORD_MIN_TTL_IN_SECONDS = 5;
const DNS_RECORD_MAX_TTL_IN_SECONDS = 30;
const IMDS_TOKEN_TTL_IN_SECONDS = 60*10;
const IMDS_TOKEN_MAX_AGE_IN_MILLISECONDS = (IMDS_TOKEN_TTL_IN_SECONDS-60)*1000;
const MAX_RETRY_DELAY_IN_SECONDS = 20;

let imdsTokenCache = undefined;
let imdsRegionCache = undefined;
let imdsCredentialsCache = undefined;
let lastRequestNo = 1;
let lastDownloadNo = 1;
const dnsCache = new LRUCache({max: 1000, ttl: DNS_RECORD_MAX_TTL_IN_SECONDS*1000});
const resolverCache = new LRUCache({max: 100});

exports.clearCache = () => {
  imdsTokenCache = undefined;
  imdsRegionCache = undefined;
  imdsCredentialsCache = undefined;
  lastRequestNo = 1;
  lastDownloadNo = 1;
  dnsCache.clear();
  resolverCache.clear();
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

function fetchResolver(timeoutInMilliseconds) {
  let resolver = resolverCache.get(timeoutInMilliseconds);
  if (resolver !== undefined) {
    return resolver;
  }
  const resolverOptions = {tries: 1};
  if (timeoutInMilliseconds > 0) {
    resolverOptions.timeout = timeoutInMilliseconds;
  }
  resolver = new Resolver(resolverOptions);
  resolverCache.set(timeoutInMilliseconds, resolver);
  return resolver;
}

function calculateQuantile(sortedNumbers, q) {
  const pos = (sortedNumbers.length - 1) * q;
  const base = Math.floor(pos);
  const rest = pos - base;
  if (sortedNumbers[base + 1] !== undefined) {
    return sortedNumbers[base] + rest * (sortedNumbers[base + 1] - sortedNumbers[base]);
  } else {
    return sortedNumbers[base];
  }
}

function calculateStats(numbers) {
  if (numbers.length === 0) {
    return {};
  }
  const count = numbers.length;
  const sum = numbers.reduce((acc, number) => acc+number, 0);
  const mean = sum / count;
  const min = Math.min(...numbers);
  const max = Math.max(...numbers);
  const sortedNumbers = numbers.toSorted(); 
  const median = calculateQuantile(sortedNumbers, 0.5);
  const p90 = calculateQuantile(sortedNumbers, 0.9);
  const p99 = calculateQuantile(sortedNumbers, 0.99);
  return {
    count,
    mean,
    median,
    p90,
    p99,
    min,
    max
  };
}

function request(nodemodule, requestOptions, body, timeoutOptions, contextOptions, cb) {
  const resolver = fetchResolver(timeoutOptions.resolveTimeoutInMilliseconds);
  const requestNo = lastRequestNo++;
  const traceId = (contextOptions.traceId) ? `${contextOptions.traceId}:request=${requestNo}` : `request=${requestNo}`;

  const abortListener = () => {
    resolver.cancel();
  };
  requestOptions.lookup = (hostname, options, cb) => {
    if (typeof options === 'function') {
      cb = options;
    }
    if (typeof options === 'number') {
      options = {family: options};
    }

    let family, fn;
    if (options.family === undefined || options.family === 4 || options.family === 'IPv4') {
      fn = 'resolve4';
      family = 4;
    } else if (options.family === 6 || options.family === 'IPv6') {
      fn = 'resolve6';
      family = 6;
    } else {
      return cb(new Error(`unsupported family: ${options.family}`));
    }

    const now = Date.now();
    const dnsCacheKey = `${family}:${hostname}`;
    const cache = dnsCache.get(dnsCacheKey);
    if (cache !== undefined) {
      let record;
      while ((record = cache.shift()) !== undefined) {
        if (record.expiredAt > now) {
          contextOptions.emitter?.emit(EVENT_NAME_REQUEST_NAME_RESOLVED, {traceId, hostname, family, address: record.address, cached: true});
          if (options.all === true) {
            return cb(null, [{address: record.address, family}]);
          } else {
            return cb(null, record.address, family);
          }
        }
      }
      dnsCache.delete(dnsCacheKey);
    }

    contextOptions.emitter?.emit(EVENT_NAME_REQUEST_NAME_RESOLVING, {traceId, hostname, family});
    resolver[fn](hostname, {ttl: true}, (err, records) => {
      if (requestOptions.signal !== undefined) {
        requestOptions.signal.removeEventListener('abort', abortListener);
      }
      if (err) {
        cb(err);
      } else {
        if (records.length === 0) {
          const enotfound = new Error('no records found');
          enotfound.code = 'ENOTFOUND';
          cb(enotfound);
        } else {
          const record = {address: records[0].address, expiredAt: now+(Math.min(Math.max(records[0].ttl, DNS_RECORD_MAX_TTL_IN_SECONDS), DNS_RECORD_MIN_TTL_IN_SECONDS)*1000)};
          if (records.length > 1) {
            dnsCache.set(dnsCacheKey, records.slice(1).map(record => ({address: record.address, expiredAt: now+(Math.min(Math.max(record.ttl, DNS_RECORD_MAX_TTL_IN_SECONDS), DNS_RECORD_MIN_TTL_IN_SECONDS)*1000)})));
          }
          contextOptions.emitter?.emit(EVENT_NAME_REQUEST_NAME_RESOLVED, {traceId, hostname, family, address: record.address, cached: false});
          if (options.all === true) {
            cb(null, [{address: record.address, family}]);
          } else {
            cb(null, record.address, family);
          }
        }
      }
    });
  };
  if (requestOptions.signal !== undefined) {
    requestOptions.signal.addEventListener('abort', abortListener, { once: true });
  }
  if (timeoutOptions.connectionTimeoutInMilliseconds > 0) {
    requestOptions.timeout = timeoutOptions.connectionTimeoutInMilliseconds;
  }
  let requestTimeoutId;
  let dataTimeoutId;
  let readTimeoutId;
  let writeTimeoutId;
  let readStatsIntervalId;
  let cbcalled = false;
  const clearTimeouts = () => {
    clearTimeout(requestTimeoutId);
    clearTimeout(dataTimeoutId);
    clearTimeout(readTimeoutId);
    clearTimeout(writeTimeoutId);
    clearInterval(readStatsIntervalId);
  };
  contextOptions.emitter?.emit(EVENT_NAME_REQUEST_CONNECTING, {traceId, hostname: requestOptions.hostname, method: requestOptions.method, path: requestOptions.path});
  const req = nodemodule.request(requestOptions, (res) => {
    contextOptions.emitter?.emit(EVENT_NAME_REQUEST_CONNECTED, {traceId});
    if (timeoutOptions.readTimeoutInMilliseconds > 0) {
      readTimeoutId = setTimeout(() => {
        clearTimeouts();
        res.destroy(new ReadTimeoutError());
      }, timeoutOptions.readTimeoutInMilliseconds);
    }
    let size = ('content-length' in res.headers) ? parseInt(res.headers['content-length'], 10) : 0;
    const bodyChunks = ('content-length' in res.headers) ? null : [];
    const bodyBuffer = ('content-length' in res.headers) ? Buffer.allocUnsafe(size) : null;
    let bodyBufferOffset = 0;
    const resetDataTimeout = () => {
      if (timeoutOptions.dataTimeoutInMilliseconds > 0) {
        clearTimeout(dataTimeoutId);
        dataTimeoutId = setTimeout(() => {
          clearTimeouts();
          res.destroy(new DataTimeoutError());
        }, timeoutOptions.dataTimeoutInMilliseconds);
      }
    };
    contextOptions.emitter?.emit(EVENT_NAME_REQUEST_BODY_READING, {traceId});
    let readStatsBytes = 0;
    let readStatsTimestamp = Date.now();
    let readStatsIntervalBytesPerSecond = [];
    const endStatsInterval = () => {
      const now = Date.now();
      const sampleWindowInMilliseconds = now-readStatsTimestamp;
      const sampleWindowInSeconds = sampleWindowInMilliseconds/1000;
      readStatsIntervalBytesPerSecond.push(readStatsBytes/sampleWindowInSeconds);
      readStatsTimestamp = now;
      readStatsBytes = 0;
      return calculateStats(readStatsIntervalBytesPerSecond);
    };
    readStatsIntervalId = setInterval(() => {
      const bytesPerSecond = endStatsInterval();
      contextOptions.emitter?.emit(EVENT_NAME_REQUEST_BODY_READING_PROGRESS, {traceId, bytesPerSecond});
    }, READ_STATS_SAMPLE_RATE_IN_SECONDS*1000);
    resetDataTimeout();
    res.on('data', chunk => {
      resetDataTimeout();
      readStatsBytes += chunk.length;
      if (bodyChunks !== null) {
        bodyChunks.push(chunk);
        size += chunk.length;
      } else {
        chunk.copy(bodyBuffer, bodyBufferOffset);
        bodyBufferOffset += chunk.length;
      }
    });
    res.on('end', () => {
      const bytesPerSecond = endStatsInterval();
      contextOptions.emitter?.emit(EVENT_NAME_REQUEST_BODY_READ, {traceId, bytesPerSecond});
      clearTimeouts();
      if (cbcalled === false) {
        cbcalled = true;
        if (bodyChunks !== null) {
          cb(null, res, Buffer.concat(bodyChunks));
        } else {
          cb(null, res, bodyBuffer);
        }
      }
    });
  });
  req.once('error', (err) => {
    clearTimeouts();
    if (cbcalled === false) {
      cbcalled = true;
      cb(err);
    }
  });
  req.once('timeout', () => {
    clearTimeouts();
    req.destroy(new ConnectionTimeoutError());
  });
  if (timeoutOptions.requestTimeoutInMilliseconds > 0) {
    requestTimeoutId = setTimeout(() => {
      req.destroy(new RequestTimeoutError());
    }, timeoutOptions.requestTimeoutInMilliseconds);
  }
  if (Buffer.isBuffer(body) && body.length > 0) {
    if (timeoutOptions.writeTimeoutInMilliseconds > 0) {
      writeTimeoutId = setTimeout(() => {
        clearTimeouts();
        req.destroy(new WriteTimeoutError());
      }, timeoutOptions.writeTimeoutInMilliseconds);
    }
    contextOptions.emitter?.emit(EVENT_NAME_REQUEST_BODY_WRITING, {traceId});
    req.write(body, () => {
      contextOptions.emitter?.emit(EVENT_NAME_REQUEST_BODY_WRITTEN, {traceId});
      clearTimeout(writeTimeoutId);
    });
  }
  req.end();
}
exports.request = request;

function retryrequest(nodemodule, requestOptions, body, retryOptions, timeoutOptions, contextOptions, cb) {
  const getTraceId = (attempt) => (contextOptions.traceId) ? `${contextOptions.traceId}:attempt=${attempt}` : `attempt=${attempt}`;
  const retry = (erredAttempt, err) => {
    const attempt = erredAttempt+1;
    if (attempt <= retryOptions.maxAttempts) {
      const maxRetryDelayInSeconds = ('maxRetryDelayInSeconds' in retryOptions) ? retryOptions.maxRetryDelayInSeconds : MAX_RETRY_DELAY_IN_SECONDS;
      const delayInMilliseconds = Math.min(Math.random() * Math.pow(2, attempt-1), maxRetryDelayInSeconds) * 1000;
      contextOptions.emitter?.emit(EVENT_NAME_REQUEST_RETRYING, {traceId: getTraceId(attempt), attempt, delayInMilliseconds, err});
      const abortListener = () => {
        clearTimeout(timeoutId);
        cb(requestOptions.signal.reason);
      };
      const timeoutId = setTimeout(() => {
        if (requestOptions.signal !== undefined) {
          requestOptions.signal.removeEventListener('abort', abortListener);
        }
        req(attempt);
      }, delayInMilliseconds);
      if (requestOptions.signal !== undefined) {
        requestOptions.signal.addEventListener('abort', abortListener, { once: true });
      }
    } else {
      cb(err);
    }
  };
  const req = (attempt) => {
    request(nodemodule, requestOptions, body, timeoutOptions, {...contextOptions, traceId: getTraceId(attempt)}, (err, res, body) => {
      if (err) {
        if (RETRIABLE_NETWORK_ERROR_CODES.includes(err.code) || RETRIABLE_ERROR_NAMES.includes(err.name)) {
          retry(attempt, err);
        } else {
          cb(err);
        }
      } else {
        if (res.statusCode == 429 || (res.statusCode >= 500 && res.statusCode <= 599)) {
          if (res.headers['content-type'] === 'application/xml') {
            const err = new Error(`status code: ${res.statusCode}\n${body.toString('utf8')}`);
            err.statusCode = res.statusCode;
            err.body = body;
            retry(attempt, err);
          } else {
            let message = `status code: ${res.statusCode}`;
            if (res.headers['content-type']) {
              message += `, content-type: ${res.headers['content-type']}`;
            }
            const err = new Error(message);
            err.statusCode = res.statusCode;
            err.body = body;
            retry(attempt, err);
          }
        } else {
          cb(null, res, body);
        }
      }
    });
  };
  req(1);
}
exports.retryrequest = retryrequest;

function imdsRequest(method, path, headers, timeoutOptions, cb) {
  const options = {
    hostname: '169.254.169.254',
    method,
    path,
    headers
  };
  retryrequest(http, options, undefined, {maxAttempts: 3}, timeoutOptions, {}, (err, res, body) => {
    if (err) {
      cb(err);
    } else {
      if (res.statusCode === 200) {
        cb(null, body.toString('utf8'));
      } else {
        const err = new Error(`unexpected IMDS status code: ${res.statusCode}.\n${body.toString('utf8')}`);
        err.statusCode = res.statusCode;
        err.body = body;
        cb(err);
      }
    }
  });
}

function refreshImdsToken(timeoutOptions) {
  imdsTokenCache = new Promise((resolve, reject) => {
    imdsRequest('PUT', '/latest/api/token', {'X-aws-ec2-metadata-token-ttl-seconds': `${IMDS_TOKEN_TTL_IN_SECONDS}`}, timeoutOptions, (err, token) => {
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

function getImdsToken(timeoutOptions, cb) {
  if (imdsTokenCache === undefined) {
    refreshImdsToken(timeoutOptions).then(({token}) => cb(null, token)).catch(cb);
  } else {
    imdsTokenCache.then(({token, cachedAt}) => {
      if ((Date.now()-cachedAt) > IMDS_TOKEN_MAX_AGE_IN_MILLISECONDS) {
        imdsTokenCache = undefined;
        getImdsToken(timeoutOptions, cb);
      } else {
        cb(null, token);
      }
    }).catch(cb);
  }
}

function imds(path, timeoutOptions, cb) {
  getImdsToken(timeoutOptions, (err, token) => {
    if (err) {
      cb(err);
    } else {
      imdsRequest('GET', path, {'X-aws-ec2-metadata-token': token}, timeoutOptions, cb);
    }
  });
}
exports.imds = imds;

const DEFAULT_IMDS_TIMEOUT_OPTIONS = {
  requestTimeoutInMilliseconds: 3000,
  resolveTimeoutInMilliseconds: 0,
  connectionTimeoutInMilliseconds: 3000,
  readTimeoutInMilliseconds: 0,
  dataTimeoutInMilliseconds: 0,
  writeTimeoutInMilliseconds: 0
};

function refreshAwsRegion() {
  imdsRegionCache = new Promise((resolve, reject) => {
    if ('AWS_REGION' in process.env) {
      resolve(process.env.AWS_REGION);
    } else {
      imds('/latest/dynamic/instance-identity/document', DEFAULT_IMDS_TIMEOUT_OPTIONS, (err, body) => {
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

function getAwsRegion(cb) {
  if (imdsRegionCache === undefined) {
    refreshAwsRegion().then(region => cb(null, region)).catch(cb);
  } else {
    imdsRegionCache.then(region => cb(null, region)).catch(cb);
  }
}

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
      imds('/latest/meta-data/iam/security-credentials/', DEFAULT_IMDS_TIMEOUT_OPTIONS, (err, body) => {
        if (err) {
          reject(err);
        } else {
          const roleName = body.trim();
          imds(`/latest/meta-data/iam/security-credentials/${roleName}`, DEFAULT_IMDS_TIMEOUT_OPTIONS, (err, body) => {
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

function getAwsCredentials(v2AwsSdkCredentials, v3AwsSdkCredentials, cb) {
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
  } else if (!(v3AwsSdkCredentials === undefined || v3AwsSdkCredentials === null)) {
    v3AwsSdkCredentials()
      .then((res) => {
        const credentials = {
          accessKeyId: res.accessKeyId,
          secretAccessKey: res.secretAccessKey
        };
        if (res.sessionToken) {
          credentials.sessionToken = res.sessionToken;
        }
        cb(null, credentials);
      })
      .catch((err) => {
        cb(err);
      });
  } else if (imdsCredentialsCache === undefined) {
    refreshAwsCredentials().then(credentials => cb(null, credentials)).catch(cb);
  } else {
    imdsCredentialsCache.then(credentials => {
      if ((Date.now()-credentials.cachedAt) > AWS_CREDENTIALS_MAX_AGE_IN_MILLISECONDS) {
        imdsCredentialsCache = undefined;
        getAwsCredentials(v2AwsSdkCredentials, v3AwsSdkCredentials, cb);
      } else {
        cb(null, credentials);
      }
    }).catch(cb);
  }
}

function getHostname(region, endpointHostname, bucket, cb) {
  const next = (region) => {
    if (bucket.includes('.')) { // virtual-hosted-style is broken for buckets with a dot, fallback to path-style
      cb(null, `s3.${region}.amazonaws.com`);
    } else {
      cb(null, `${bucket}.s3.${region}.amazonaws.com`);
    }
  };
  if (endpointHostname === undefined || endpointHostname === null) {
    if (region === undefined || region === null) {
      getAwsRegion((err, region) => {
        if (err) {
          cb(err);
        } else {
          next(region);
        }
      });
    } else {
      next(region);
    }
  } else {
    cb(null, endpointHostname);
  }
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

function getObject(params, s3Options, retryOptions, timeoutOptions, contextOptions, cb) {
  const {Bucket, Key, VersionId, PartNumber, Range} = params;
  const {region, v2AwsSdkCredentials, endpointHostname, agent, v3AwsSdkCredentials} = s3Options;
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
  getHostname(region, endpointHostname, Bucket, (err, hostname) => {
    if (err) {
      cb(err);
    } else {
      getAwsCredentials(v2AwsSdkCredentials, v3AwsSdkCredentials, (err, credentials) => {
        if (err) {
          cb(err);
        } else {
          let path = '';
          if (!hostname.startsWith(`${Bucket}.`)) { // fallback to path-style if hostname does not start with bucket aka virtual-hosted–style
            path += `/${Bucket}`;
          }
          path += `/${escapeKey(Key)}?${querystring.stringify(qs)}`;
          const options = aws4.sign({
            hostname,
            method: 'GET',
            path,
            headers,
            service: 's3',
            signal: ac.signal,
            agent
          }, credentials);
          retryrequest(https, options, undefined, retryOptions, timeoutOptions, contextOptions, (err, res, body) => {
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
                        if ((result.Error.Code === 'PermanentRedirect' || result.Error.Code === 'IllegalLocationConstraintException') && res.headers['x-amz-bucket-region']) { // TODO use region in all further parts to avoid running into this error for every part
                          getObject(params, {...s3Options, region: res.headers['x-amz-bucket-region']}, retryOptions, timeoutOptions, contextOptions, cb);
                        } else if (result.Error.Code === 'PermanentRedirect' && result.Error.Endpoint) { // TODO use endpoint in all further parts to avoid running into this error for every part
                          getObject(params, {...s3Options, endpointHostname: result.Error.Endpoint}, retryOptions, timeoutOptions, contextOptions, cb);
                        } else {
                          const err = new Error(`${result.Error.Code}: ${result.Error.Message}`);
                          err.statusCode = res.statusCode;
                          err.body = body;
                          err.code = result.Error.Code;
                          cb(err);
                        }
                      } else {
                        const err = new Error(`unexpected S3 XML response (${res.statusCode}):\n${body.toString('utf8')}`);
                        err.statusCode = res.statusCode;
                        err.body = body;
                        cb(err);
                      }
                    }
                  });
                } else {
                  const err = new Error(`unexpected S3 response (${res.statusCode}, ${res.headers['content-type']})`);
                  err.statusCode = res.statusCode;
                  err.body = body;
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

exports.download = ({bucket, key, version}, {partSizeInMegabytes, concurrency, requestTimeoutInMilliseconds, resolveTimeoutInMilliseconds, connectionTimeoutInMilliseconds, readTimeoutInMilliseconds, dataTimeoutInMilliseconds, writeTimeoutInMilliseconds, region, v2AwsSdkCredentials, endpointHostname, agent, v3AwsSdkCredentials}) => {
  if (concurrency < 1) {
    throw new Error('concurrency > 0');
  }

  if (requestTimeoutInMilliseconds === undefined || requestTimeoutInMilliseconds === null) {
    requestTimeoutInMilliseconds = 300000;
  } else if (requestTimeoutInMilliseconds < 0) {
    throw new Error('requestTimeoutInMilliseconds >= 0');
  }

  if (resolveTimeoutInMilliseconds === undefined || resolveTimeoutInMilliseconds === null) {
    resolveTimeoutInMilliseconds = 3000;
  } else if (resolveTimeoutInMilliseconds < 0) {
    throw new Error('resolveTimeoutInMilliseconds >= 0');
  }

  if (connectionTimeoutInMilliseconds === undefined || connectionTimeoutInMilliseconds === null) {
    connectionTimeoutInMilliseconds = 3000;
  } else if (connectionTimeoutInMilliseconds < 0) {
    throw new Error('connectionTimeoutInMilliseconds >= 0');
  }

  if (readTimeoutInMilliseconds === undefined || readTimeoutInMilliseconds === null) {
    readTimeoutInMilliseconds = 300000;
  } else if (readTimeoutInMilliseconds < 0) {
    throw new Error('readTimeoutInMilliseconds >= 0');
  }

  if (dataTimeoutInMilliseconds === undefined || dataTimeoutInMilliseconds === null) {
    dataTimeoutInMilliseconds = 3000;
  } else if (dataTimeoutInMilliseconds < 0) {
    throw new Error('dataTimeoutInMilliseconds >= 0');
  }

  if (writeTimeoutInMilliseconds === undefined || writeTimeoutInMilliseconds === null) {
    writeTimeoutInMilliseconds = 300000;
  } else if (writeTimeoutInMilliseconds < 0) {
    throw new Error('writeTimeoutInMilliseconds >= 0');
  }

  if (!(v2AwsSdkCredentials === undefined || v2AwsSdkCredentials === null)) {
    if (typeof v2AwsSdkCredentials.get !== 'function') {
      throw new Error('invalid v2AwsSdkCredentials');
    }
  }

  if (!(v3AwsSdkCredentials === undefined || v3AwsSdkCredentials === null)) {
    if (typeof v3AwsSdkCredentials !== 'function') {
      throw new Error('invalid v3AwsSdkCredentials');
    }
  }

  const s3Options = {region, v2AwsSdkCredentials, endpointHostname, agent, v3AwsSdkCredentials};
  const retryOptions = {maxAttempts: 5};
  const timeoutOptions = {requestTimeoutInMilliseconds, resolveTimeoutInMilliseconds, connectionTimeoutInMilliseconds, readTimeoutInMilliseconds, dataTimeoutInMilliseconds, writeTimeoutInMilliseconds};
  const downloadNo = lastDownloadNo++;

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
      if (stream !== null) {
        stream.destroy(err);
      }
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

  function getTraceId(partNo) {
    if (partNo !== undefined) {
      return `download=${downloadNo}:part=${partNo}`;
    } else {
      return `download=${downloadNo}`;
    }
  }

  function writePart(partNo, chunk, cb) {
    if (lastWrittenPartNo === (partNo-1)) {
      emitter.emit(EVENT_NAME_PART_WRITING, {traceId: getTraceId(partNo), partNo});
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
    const req = getObject(params, s3Options, retryOptions, timeoutOptions, {emitter, traceId: getTraceId(partNo)}, (err, data) => {
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
      emitter.emit(EVENT_NAME_PART_DOWNLOADING, {traceId: getTraceId(partNo), partNo});
      downloadPart(partNo, (err, data) => {
        if (err) {
          abortDownloads(err);
        } else {
          emitter.emit(EVENT_NAME_PART_DOWNLOADED, {traceId: getTraceId(partNo), partNo});
          writePart(partNo, data.Body, () => {
            emitter.emit(EVENT_NAME_PART_DONE, {traceId: getTraceId(partNo), partNo});
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
        partsDownloading[1] = getObject(params, s3Options, retryOptions, timeoutOptions,  {emitter, traceId: `download=${downloadNo}:part=1`}, (err, data) => {
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
    emitter.emit(EVENT_NAME_PART_DOWNLOADING, {traceId: getTraceId(1), partNo: 1});
    meta((err, metadata, body) => {
      if (err) {
        stream.destroy(err);
      } else {
        emitter.emit(EVENT_NAME_OBJECT_DOWNLOADING, {traceId: getTraceId(), ...metadata});
        if (partSizeInBytes === null) {
          emitter.emit(EVENT_NAME_PART_DOWNLOADED, {traceId: getTraceId(1), partNo: 1});
          if ('parts' in metadata && metadata.parts > 1) {
            emitter.emit(EVENT_NAME_PART_WRITING, {traceId: getTraceId(1), partNo: 1});
            write(body, () => {
              emitter.emit(EVENT_NAME_PART_DONE, {traceId: getTraceId(1), partNo: 1});
              lastWrittenPartNo = 1;
              nextPartNo = 2;
              partsToDownload = metadata.parts;
              startDownloadingParts();
            });
          } else {
            emitter.emit(EVENT_NAME_PART_WRITING, {traceId: getTraceId(1), partNo: 1});
            stream.end(body, () => {
              emitter.emit(EVENT_NAME_PART_DONE, {traceId: getTraceId(1), partNo: 1});
            });
          }
        } else {
          emitter.emit(EVENT_NAME_PART_DOWNLOADED, {traceId: getTraceId(1), partNo: 1});
          bytesToDownload = metadata.lengthInBytes;
          if (bytesToDownload <= partSizeInBytes) {
            emitter.emit(EVENT_NAME_PART_WRITING, {traceId: getTraceId(1), partNo: 1});
            stream.end(body, () => {
              emitter.emit(EVENT_NAME_PART_DONE, {traceId: getTraceId(1), partNo: 1});
            });
          } else {
            emitter.emit(EVENT_NAME_PART_WRITING, {traceId: getTraceId(1), partNo: 1});
            write(body, () => {
              emitter.emit(EVENT_NAME_PART_DONE, {traceId: getTraceId(1), partNo: 1});
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
        started = true;
        stream = new PassThrough();
        start();
      }
      return stream;
    },
    file: (path, cb) => {
      if (started === false)  {
        started = true;
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
