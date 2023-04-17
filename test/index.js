const assert = require('assert');
const {pipeline} = require('node:stream');
const http = require('node:http');
const fs = require('node:fs');
const mockfs = require('mock-fs');
const nock = require('nock');
const {download, clearCache, request} = require('../index.js');

function nockPart(partSize, partNumber, parts, bytes, optionalTimeout) {
  const headers = {
    'Content-Length': `${partSize}`,
    'Content-Range': `bytes ${(partNumber-1)*partSize}-${partNumber*partSize-1}/${bytes}`
  };
  if (parts > 1) {
    headers['x-amz-mp-parts-count'] = `${parts}`;
  }
  const n = nock('https://bucket.s3.eu-west-1.amazonaws.com', {
    reqheaders: {
      'x-amz-content-sha256': /.*/,
      'x-amz-date': /.*/,
      authorization: /.*/
    }
  })
    .get('/key')
    .query({
      versionId: 'version',
      partNumber: `${partNumber}`
    });
  if (optionalTimeout !== undefined) {
    n.delay(optionalTimeout);
  }
  n.reply(206, Buffer.alloc(partSize), headers);
  return n;
}

function nockRange(startByte, endByte, bytes, optionalTimeout) {
  const size = Math.min(endByte-startByte+1, bytes);
  const n = nock('https://bucket.s3.eu-west-1.amazonaws.com', {
    reqheaders: {
      range: `bytes=${startByte}-${endByte}`,
      'x-amz-content-sha256': /.*/,
      'x-amz-date': /.*/,
      authorization: /.*/
    }
  })
    .get('/key')
    .query({
      versionId: 'version'
    });
  if (optionalTimeout !== undefined) {
    n.delay(optionalTimeout);
  }
  n.reply(206, Buffer.alloc(size), {
    'Content-Length': `${size}`,
    'Content-Range': `bytes ${startByte}-${endByte}/${bytes}`
  });
  return n;
}

function nockImds() {
  const responseBodyToken = 'TOKEN';
  const responseBodyDocument = JSON.stringify({region: 'eu-west-1'});
  const responseBodyRole = 'ROLE\n';
  const responseBodyCredentials = JSON.stringify({
    accessKeyId: 'AWS_ACCESS_KEY_ID',
    secretAccessKey: 'AWS_SECRET_ACCESS_KEY',
    sessionToken: 'AWS_SESSION_TOKEN'
  });
  nock('http://169.254.169.254', {
    reqheaders: {
      'X-aws-ec2-metadata-token-ttl-seconds': '60'
    }
  })
    .put('/latest/api/token')
    .times(3)
    .reply(200, responseBodyToken, {'content-length': Buffer.byteLength(responseBodyToken, 'utf8')});
  nock('http://169.254.169.254', {
    reqheaders: {
      'X-aws-ec2-metadata-token': responseBodyToken
    }
  })
    .get('/latest/dynamic/instance-identity/document')
    .reply(200, responseBodyDocument, {'content-length': Buffer.byteLength(responseBodyDocument, 'utf8')})
    .get('/latest/meta-data/iam/security-credentials/')
    .reply(200, responseBodyRole, {'content-length': Buffer.byteLength(responseBodyRole, 'utf8')})
    .get('/latest/meta-data/iam/security-credentials/ROLE')
    .reply(200, responseBodyCredentials, {'content-length': Buffer.byteLength(responseBodyCredentials, 'utf8')});
}

describe('index', () => {
  describe('request', () => {
    before(() => {
      nock.disableNetConnect();
    });
    after(() => {
      nock.enableNetConnect();
    });
    afterEach(() => {
    });
    it('with content-length = 0', (done) => {
      nock('http://localhost')
        .get('/test')
        .reply(204, '', {'Content-Type': 'application/text', 'Content-Length': '0'});
      request(http, {
        hostname: 'localhost',
        method: 'GET',
        path: '/test'
      }, null, (err, res, body) => {
        if (err) {
          done(err);
        } else {
          assert.deepStrictEqual(body.toString('utf8'), '');
          done();
        }
      });
    });
    it('with content-length > 0', (done) => {
      nock('http://localhost')
        .get('/test')
        .reply(200, 'Hello world!', {'Content-Type': 'application/text', 'Content-Length': '12'});
      request(http, {
        hostname: 'localhost',
        method: 'GET',
        path: '/test'
      }, null, (err, res, body) => {
        if (err) {
          done(err);
        } else {
          assert.deepStrictEqual(body.toString('utf8'), 'Hello world!');
          done();
        }
      });
    });
    it('without content-length', (done) => {
      nock('http://localhost')
        .get('/test')
        .reply(200, 'Hello world!', {'Content-Type': 'application/text'});
      request(http, {
        hostname: 'localhost',
        method: 'GET',
        path: '/test'
      }, null, (err, res, body) => {
        if (err) {
          done(err);
        } else {
          assert.deepStrictEqual(body.toString('utf8'), 'Hello world!');
          done();
        }
      });
    });
  });
  describe('credentials via environment variables', () => {
    before(() => {
      nock.disableNetConnect();
      process.env.AWS_REGION = 'eu-west-1';
      process.env.AWS_ACCESS_KEY_ID = 'AWS_ACCESS_KEY_ID';
      process.env.AWS_SECRET_ACCESS_KEY = 'AWS_SECRET_ACCESS_KEY';
    });
    after(() => {
      nock.enableNetConnect();
      delete process.env.AWS_REGION;
      delete process.env.AWS_ACCESS_KEY_ID;
      delete process.env.AWS_SECRET_ACCESS_KEY;
    });
    afterEach(() => {
      mockfs.restore();
      nock.cleanAll();
      clearCache();
    });
    describe('readStream', () => {
      describe('without partSizeInMegabytes', () => {
        describe('one part', () => {
          describe('error', () => {
            it('NoSuchBucket', (done) => {
              nock('https://bucket.s3.eu-west-1.amazonaws.com', {
                reqheaders: {
                  'x-amz-content-sha256': /.*/,
                  'x-amz-date': /.*/,
                  authorization: /.*/
                }
              })
                .get('/key')
                .query({
                  versionId: 'version',
                  partNumber: '1'
                })
                .reply(404, '<?xml version="1.0" encoding="UTF-8"?>\n<Error><Code>NoSuchBucket</Code><Message>The specified bucket does not exist</Message><BucketName>bucketav-clean-files2</BucketName><RequestId>QRN2ST0SDNGGGMCD</RequestId><HostId>JiyCd7RRDjasxosfjcsggJiZm6ukcqJLb/wQ7n0K07BzKkJ8qhfIu/wfCNyroNCx/ET8TOjm0Rg=</HostId></Error>', {'x-amz-request-id': 'QRN2ST0SDNGGGMCD', 'x-amz-id-2': 'JiyCd7RRDjasxosfjcsggJiZm6ukcqJLb/wQ7n0K07BzKkJ8qhfIu/wfCNyroNCx/ET8TOjm0Rg=', 'Content-Type': 'application/xml', 'Transfer-Encoding': 'chunked', 'Date': 'Wed, 05 Apr 2023 07:06:23 GMT', 'Server': 'AmazonS3'});
              mockfs({
                '/tmp': {
                }
              });
              pipeline(
                download({bucket:'bucket', key: 'key', version: 'version'}, {concurrency: 4}).readStream(),
                fs.createWriteStream('/tmp/test'),
                (err) => {
                  if (err) {
                    assert.ok(nock.isDone());
                    assert.deepStrictEqual(err.code, 'NoSuchBucket');
                    assert.deepStrictEqual(err.message, 'NoSuchBucket: The specified bucket does not exist');
                    done();
                  } else {
                    assert.fail();
                  }
                }
              );
            });
            it('NoSuchKey', (done) => {
              nock('https://bucket.s3.eu-west-1.amazonaws.com', {
                reqheaders: {
                  'x-amz-content-sha256': /.*/,
                  'x-amz-date': /.*/,
                  authorization: /.*/
                }
              })
                .get('/key')
                .query({
                  versionId: 'version',
                  partNumber: '1'
                })
                .reply(404, '<?xml version="1.0" encoding="UTF-8"?>\n<Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message><Key>2GB.bin</Key><RequestId>S5XB48D3GN8PFEHB</RequestId><HostId>Xse1YNlihaJ+G5oViGxs1m1ec4OnKJoIRxB45ha2yByUPEF38+Z/3bOfHPGCdMBFmFQxmgDXVfY=</HostId></Error>', {'x-amz-request-id': 'S5XB48D3GN8PFEHB', 'x-amz-id-2': 'Xse1YNlihaJ+G5oViGxs1m1ec4OnKJoIRxB45ha2yByUPEF38+Z/3bOfHPGCdMBFmFQxmgDXVfY=', 'Content-Type': 'application/xml', 'Transfer-Encoding': 'chunked', 'Date': 'Wed, 05 Apr 2023 07:13:52 GMT', 'Server': 'AmazonS3'});
              mockfs({
                '/tmp': {
                }
              });
              pipeline(
                download({bucket:'bucket', key: 'key', version: 'version'}, {concurrency: 4}).readStream(),
                fs.createWriteStream('/tmp/test'),
                (err) => {
                  if (err) {
                    assert.ok(nock.isDone());
                    assert.deepStrictEqual(err.code, 'NoSuchKey');
                    assert.deepStrictEqual(err.message, 'NoSuchKey: The specified key does not exist.');
                    done();
                  } else {
                    assert.fail();
                  }
                }
              );
            });
            it('AccessDenied', (done) => {
              nock('https://bucket.s3.eu-west-1.amazonaws.com', {
                reqheaders: {
                  'x-amz-content-sha256': /.*/,
                  'x-amz-date': /.*/,
                  authorization: /.*/
                }
              })
                .get('/key')
                .query({
                  versionId: 'version',
                  partNumber: '1'
                })
                .reply(403, '<?xml version="1.0" encoding="UTF-8"?>\n<Error><Code>AccessDenied</Code><Message>Access Denied</Message><RequestId>KJBCKTP2T7C9E4S4</RequestId><HostId>oQvFUmi5gAQc3cj/go4bUeYfuSy9uDgcStq8a21HeXGM1Wc9+PbeK/05zFbCTDSpQS3GLVs1cHh+T8SCG5cl6Q==</HostId></Error>', {'x-amz-request-id': 'KJBCKTP2T7C9E4S4', 'x-amz-id-2': 'oQvFUmi5gAQc3cj/go4bUeYfuSy9uDgcStq8a21HeXGM1Wc9+PbeK/05zFbCTDSpQS3GLVs1cHh+T8SCG5cl6Q==', 'Content-Type': 'application/xml', 'Transfer-Encoding': 'chunked', 'Date': 'Wed, 05 Apr 2023 07:14:57 GMT', 'Server': 'AmazonS3'});
              mockfs({
                '/tmp': {
                }
              });
              pipeline(
                download({bucket:'bucket', key: 'key', version: 'version'}, {concurrency: 4}).readStream(),
                fs.createWriteStream('/tmp/test'),
                (err) => {
                  if (err) {
                    assert.ok(nock.isDone());
                    assert.deepStrictEqual(err.code, 'AccessDenied');
                    assert.deepStrictEqual(err.message, 'AccessDenied: Access Denied');
                    done();
                  } else {
                    assert.fail();
                  }
                }
              );
            });
            it('download error', (done) => {
              nock('https://bucket.s3.eu-west-1.amazonaws.com', {
                reqheaders: {
                  'x-amz-content-sha256': /.*/,
                  'x-amz-date': /.*/,
                  authorization: /.*/
                }
              })
                .get('/key')
                .query({
                  versionId: 'version',
                  partNumber: '1'
                })
                .reply(403);
              mockfs({
                '/tmp': {
                }
              });
              pipeline(
                download({bucket:'bucket', key: 'key', version: 'version'}, {concurrency: 4}).readStream(),
                fs.createWriteStream('/tmp/test'),
                (err) => {
                  if (err) {
                    assert.ok(nock.isDone());
                    assert.deepStrictEqual(err.message, 'unexpected S3 response (403, undefined)');
                    done();
                  } else {
                    assert.fail();
                  }
                }
              );
            });
          });
          it('happy', (done) => {
            const bytes = 1000000;
            nockPart(1000000, 1, 1, bytes);
            mockfs({
              '/tmp': {
              }
            });
            pipeline(
              download({bucket:'bucket', key: 'key', version: 'version'}, {concurrency: 4}).readStream(),
              fs.createWriteStream('/tmp/test'),
              (err) => {
                if (err) {
                  done(err);
                } else {
                  assert.ok(nock.isDone());
                  const {size} = fs.statSync('/tmp/test');
                  assert.deepStrictEqual(size, bytes);
                  done();
                }
              }
            );
          });
          it('abort', (done) => {
            const bytes = 1000000;
            nockPart(1000000, 1, 1, bytes, 200);
            mockfs({
              '/tmp': {
              }
            });
            const d = download({bucket:'bucket', key: 'key', version: 'version'}, {concurrency: 4});
            setTimeout(() => {
              d.abort();
            }, 100);
            pipeline(
              d.readStream(),
              fs.createWriteStream('/tmp/test'),
              (err) => {
                if (err) {
                  assert.ok(nock.isDone());
                  assert.deepStrictEqual(err.code, 's3-getobject-accelerator:aborted');
                  done();
                } else {
                  assert.fail();
                }
              }
            );
          });
        });
        describe('multiple parts', () => {
          it('download error', (done) => {
            const bytes = 17000000;
            nockPart(8000000, 1, 3, bytes);
            nock('https://bucket.s3.eu-west-1.amazonaws.com', {
              reqheaders: {
                'x-amz-content-sha256': /.*/,
                'x-amz-date': /.*/,
                authorization: /.*/
              }
            })
              .get('/key')
              .query({
                versionId: 'version',
                partNumber: '2'
              })
              .reply(403);
            nockPart(1000000, 3, 3, bytes);
            mockfs({
              '/tmp': {
              }
            });
            pipeline(
              download({bucket:'bucket', key: 'key', version: 'version'}, {concurrency: 4}).readStream(),
              fs.createWriteStream('/tmp/test'),
              (err) => {
                if (err) {
                  assert.ok(nock.isDone());
                  assert.deepStrictEqual(err.message, 'unexpected S3 response (403, undefined)');
                  done();
                } else {
                  assert.fail();
                }
              }
            );
          });
          it('abort', (done) => {
            const bytes = 17000000;
            nockPart(8000000, 1, 3, bytes, 100);
            nockPart(8000000, 2, 3, bytes, 200);
            nockPart(1000000, 3, 3, bytes, 300);
            mockfs({
              '/tmp': {
              }
            });
            const d = download({bucket:'bucket', key: 'key', version: 'version'}, {concurrency: 4});
            setTimeout(() => {
              d.abort();
            }, 150);
            pipeline(
              d.readStream(),
              fs.createWriteStream('/tmp/test'),
              (err) => {
                if (err) {
                  assert.ok(nock.isDone());
                  assert.deepStrictEqual(err.code, 's3-getobject-accelerator:aborted');
                  done();
                } else {
                  assert.fail();
                }
              }
            );
          });
          it('number of parts < concurrency', (done) => {
            const bytes = 17000000;
            nockPart(8000000, 1, 3, bytes);
            nockPart(8000000, 2, 3, bytes);
            nockPart(1000000, 3, 3, bytes);
            mockfs({
              '/tmp': {
              }
            });
            const d = download({bucket:'bucket', key: 'key', version: 'version'}, {concurrency: 4});
            let length;
            let active = 0;
            let activeMax = 0;
            const downloadingPartNos = [];
            const downloadedPartNos = [];
            const donePartNos = [];
            d.on('object:downloading', ({lengthInBytes}) => {
              length = lengthInBytes;
            });
            d.on('part:downloading', ({partNo}) => {
              active++;
              activeMax = Math.max(activeMax, active);
              downloadingPartNos.push(partNo);
            });
            d.on('part:downloaded', ({partNo}) => {
              downloadedPartNos.push(partNo);
            });
            d.on('part:done', ({partNo}) => {
              active--;
              donePartNos.push(partNo);
            });
            pipeline(
              d.readStream(),
              fs.createWriteStream('/tmp/test'),
              (err) => {
                if (err) {
                  done(err);
                } else {
                  assert.ok(nock.isDone());
                  assert.deepStrictEqual(length, bytes);
                  const {size} = fs.statSync('/tmp/test');
                  assert.deepStrictEqual(size, bytes);
                  assert.deepStrictEqual(active, 0);
                  assert.deepStrictEqual(activeMax, 2);
                  assert.deepStrictEqual([1, 2, 3], downloadingPartNos);
                  assert.deepStrictEqual([1, 2, 3], downloadedPartNos);
                  assert.deepStrictEqual([1, 2, 3], donePartNos);
                  done();
                }
              }
            );
          });
          it('number of parts = concurrency', (done) => {
            const bytes = 33000000;
            nockPart(8000000, 1, 5, bytes, 100);
            nockPart(8000000, 2, 5, bytes, 200);
            nockPart(8000000, 3, 5, bytes, 400);
            nockPart(8000000, 4, 5, bytes, 100);
            nockPart(1000000, 5, 5, bytes, 300);
            mockfs({
              '/tmp': {
              }
            });
            const d = download({bucket:'bucket', key: 'key', version: 'version'}, {concurrency: 4});
            let active = 0;
            let activeMax = 0;
            const downloadingPartNos = [];
            const downloadedPartNos = [];
            const donePartNos = [];
            d.on('part:downloading', ({partNo}) => {
              active++;
              activeMax = Math.max(activeMax, active);
              downloadingPartNos.push(partNo);
            });
            d.on('part:downloaded', ({partNo}) => {
              downloadedPartNos.push(partNo);
            });
            d.on('part:done', ({partNo}) => {
              active--;
              donePartNos.push(partNo);
            });
            pipeline(
              d.readStream(),
              fs.createWriteStream('/tmp/test'),
              (err) => {
                if (err) {
                  done(err);
                } else {
                  assert.ok(nock.isDone());
                  const {size} = fs.statSync('/tmp/test');
                  assert.deepStrictEqual(size, bytes);
                  assert.deepStrictEqual(active, 0);
                  assert.deepStrictEqual(activeMax, 4);
                  assert.deepStrictEqual([1, 2, 3, 4, 5], downloadingPartNos);
                  assert.deepStrictEqual([1, 4, 2, 5, 3], downloadedPartNos);
                  assert.deepStrictEqual([1, 2, 3, 4, 5], donePartNos);
                  done();
                }
              }
            );
          });
          it('number of parts > concurrency', (done) => {
            const bytes = 41000000;
            nockPart(8000000, 1, 6, bytes, 100);
            nockPart(8000000, 2, 6, bytes, 500);
            nockPart(8000000, 3, 6, bytes, 400);
            nockPart(8000000, 4, 6, bytes, 200);
            nockPart(8000000, 5, 6, bytes, 300);
            nockPart(1000000, 6, 6, bytes, 100);
            mockfs({
              '/tmp': {
              }
            });
            const d = download({bucket:'bucket', key: 'key', version: 'version'}, {concurrency: 4});
            let active = 0;
            let activeMax = 0;
            const downloadingPartNos = [];
            const downloadedPartNos = [];
            const donePartNos = [];
            d.on('part:downloading', ({partNo}) => {
              active++;
              activeMax = Math.max(activeMax, active);
              downloadingPartNos.push(partNo);
            });
            d.on('part:downloaded', ({partNo}) => {
              downloadedPartNos.push(partNo);
            });
            d.on('part:done', ({partNo}) => {
              active--;
              donePartNos.push(partNo);
            });
            pipeline(
              d.readStream(),
              fs.createWriteStream('/tmp/test'),
              (err) => {
                if (err) {
                  done(err);
                } else {
                  assert.ok(nock.isDone());
                  const {size} = fs.statSync('/tmp/test');
                  assert.deepStrictEqual(size, bytes);
                  assert.deepStrictEqual(active, 0);
                  assert.deepStrictEqual(activeMax, 4);
                  assert.deepStrictEqual([1, 2, 3, 4, 5, 6], downloadingPartNos);
                  assert.deepStrictEqual([1, 4, 5, 3, 2, 6], downloadedPartNos);
                  assert.deepStrictEqual([1, 2, 3, 4, 5, 6], donePartNos);
                  done();
                }
              }
            );
          });
        });
      });
      describe('with partSizeInMegabytes', () => {
        describe('object size < part size', () => {
          it('download error', (done) => {
            nock('https://bucket.s3.eu-west-1.amazonaws.com', {
              reqheaders: {
                range: 'bytes=0-7999999',
                'x-amz-content-sha256': /.*/,
                'x-amz-date': /.*/,
                authorization: /.*/
              }
            })
              .get('/key')
              .query({
                versionId: 'version'
              })
              .reply(403);
            mockfs({
              '/tmp': {
              }
            });
            pipeline(
              download({bucket:'bucket', key: 'key', version: 'version'}, {partSizeInMegabytes: 8, concurrency: 4}).readStream(),
              fs.createWriteStream('/tmp/test'),
              (err) => {
                if (err) {
                  assert.ok(nock.isDone());
                  assert.deepStrictEqual(err.message, 'unexpected S3 response (403, undefined)');
                  done();
                } else {
                  assert.fail();
                }
              }
            );
          });
          it('happy', (done) => {
            const bytes = 1000000;
            nockRange(0, 7999999, bytes);
            mockfs({
              '/tmp': {
              }
            });
            pipeline(
              download({bucket:'bucket', key: 'key', version: 'version'}, {partSizeInMegabytes: 8, concurrency: 4}).readStream(),
              fs.createWriteStream('/tmp/test'),
              (err) => {
                if (err) {
                  done(err);
                } else {
                  assert.ok(nock.isDone());
                  const {size} = fs.statSync('/tmp/test');
                  assert.deepStrictEqual(size, bytes);
                  done();
                }
              }
            );
          });
          it('abort', (done) => {
            const bytes = 1000000;
            nockRange(0, 7999999, bytes, 200);
            mockfs({
              '/tmp': {
              }
            });
            const d = download({bucket:'bucket', key: 'key', version: 'version'}, {partSizeInMegabytes: 8, concurrency: 4});
            setTimeout(() => {
              d.abort();
            }, 100);
            pipeline(
              d.readStream(),
              fs.createWriteStream('/tmp/test'),
              (err) => {
                if (err) {
                  assert.ok(nock.isDone());
                  assert.deepStrictEqual(err.code, 's3-getobject-accelerator:aborted');
                  done();
                } else {
                  assert.fail();
                }
              }
            );
          });
        });
        it('object size = part size', (done) => {
          const bytes = 8000000;
          nockRange(0, 7999999, bytes);
          mockfs({
            '/tmp': {
            }
          });
          pipeline(
            download({bucket:'bucket', key: 'key', version: 'version'}, {partSizeInMegabytes: 8, concurrency: 4}).readStream(),
            fs.createWriteStream('/tmp/test'),
            (err) => {
              if (err) {
                done(err);
              } else {
                assert.ok(nock.isDone());
                const {size} = fs.statSync('/tmp/test');
                assert.deepStrictEqual(size, bytes);
                done();
              }
            }
          );
        });
        describe('object size > part size', () => {
          it('last part size = part size', (done) => {
            const bytes = 24000000;
            nockRange(0, 7999999, bytes);
            nockRange(8000000, 15999999, bytes);
            nockRange(16000000, 23999999, bytes);
            mockfs({
              '/tmp': {
              }
            });
            pipeline(
              download({bucket:'bucket', key: 'key', version: 'version'}, {partSizeInMegabytes: 8, concurrency: 4}).readStream(),
              fs.createWriteStream('/tmp/test'),
              (err) => {
                if (err) {
                  done(err);
                } else {
                  assert.ok(nock.isDone());
                  const {size} = fs.statSync('/tmp/test');
                  assert.deepStrictEqual(size, bytes);
                  done();
                }
              }
            );
          });
          it('download error', (done) => {
            const bytes = 24000000;
            nockRange(0, 7999999, bytes);
            nock('https://bucket.s3.eu-west-1.amazonaws.com', {
              reqheaders: {
                range: 'bytes=8000000-15999999',
                'x-amz-content-sha256': /.*/,
                'x-amz-date': /.*/,
                authorization: /.*/
              }
            })
              .get('/key')
              .query({
                versionId: 'version'
              })
              .reply(403);
            nockRange(16000000, 23999999, bytes);
            mockfs({
              '/tmp': {
              }
            });
            pipeline(
              download({bucket:'bucket', key: 'key', version: 'version'}, {partSizeInMegabytes: 8, concurrency: 4}).readStream(),
              fs.createWriteStream('/tmp/test'),
              (err) => {
                if (err) {
                  assert.ok(nock.isDone());
                  assert.deepStrictEqual(err.message, 'unexpected S3 response (403, undefined)');
                  done();
                } else {
                  assert.fail();
                }
              }
            );
          });
          it('last part size < part size', (done) => {
            const bytes = 17000000;
            nockRange(0, 7999999, bytes);
            nockRange(8000000, 15999999, bytes);
            nockRange(16000000, 16999999, bytes);
            mockfs({
              '/tmp': {
              }
            });
            pipeline(
              download({bucket:'bucket', key: 'key', version: 'version'}, {partSizeInMegabytes: 8, concurrency: 4}).readStream(),
              fs.createWriteStream('/tmp/test'),
              (err) => {
                if (err) {
                  done(err);
                } else {
                  assert.ok(nock.isDone());
                  const {size} = fs.statSync('/tmp/test');
                  assert.deepStrictEqual(size, bytes);
                  done();
                }
              }
            );
          });
          it('number of parts < concurrency', (done) => {
            const bytes = 17000000;
            nockRange(0, 7999999, bytes, 100);
            nockRange(8000000, 15999999, bytes, 100);
            nockRange(16000000, 16999999, bytes, 200);
            mockfs({
              '/tmp': {
              }
            });
            const d = download({bucket:'bucket', key: 'key', version: 'version'}, {partSizeInMegabytes: 8, concurrency: 4});
            let length;
            let active = 0;
            let activeMax = 0;
            const downloadingPartNos = [];
            const downloadedPartNos = [];
            const donePartNos = [];
            d.on('object:downloading', ({lengthInBytes}) => {
              length = lengthInBytes;
            });
            d.on('part:downloading', ({partNo}) => {
              active++;
              activeMax = Math.max(activeMax, active);
              downloadingPartNos.push(partNo);
            });
            d.on('part:downloaded', ({partNo}) => {
              downloadedPartNos.push(partNo);
            });
            d.on('part:done', ({partNo}) => {
              active--;
              donePartNos.push(partNo);
            });
            pipeline(
              d.readStream(),
              fs.createWriteStream('/tmp/test'),
              (err) => {
                if (err) {
                  done(err);
                } else {
                  assert.ok(nock.isDone());
                  assert.deepStrictEqual(length, bytes);
                  const {size} = fs.statSync('/tmp/test');
                  assert.deepStrictEqual(size, bytes);
                  assert.deepStrictEqual(active, 0);
                  assert.deepStrictEqual(activeMax, 2);
                  assert.deepStrictEqual([1, 2, 3], downloadingPartNos);
                  assert.deepStrictEqual([1, 2, 3], downloadedPartNos);
                  assert.deepStrictEqual([1, 2, 3], donePartNos);
                  done();
                }
              }
            );
          });
          it('number of parts = concurrency', (done) => {
            const bytes = 33000000;
            nockRange(0, 7999999, bytes, 100);
            nockRange(8000000, 15999999, bytes, 200);
            nockRange(16000000, 23999999, bytes, 400);
            nockRange(24000000, 31999999, bytes, 100);
            nockRange(32000000, 32999999, bytes, 300);
            mockfs({
              '/tmp': {
              }
            });
            const d = download({bucket:'bucket', key: 'key', version: 'version'}, {partSizeInMegabytes: 8, concurrency: 4});
            let active = 0;
            let activeMax = 0;
            const downloadingPartNos = [];
            const downloadedPartNos = [];
            const donePartNos = [];
            d.on('part:downloading', ({partNo}) => {
              active++;
              activeMax = Math.max(activeMax, active);
              downloadingPartNos.push(partNo);
            });
            d.on('part:downloaded', ({partNo}) => {
              downloadedPartNos.push(partNo);
            });
            d.on('part:done', ({partNo}) => {
              active--;
              donePartNos.push(partNo);
            });
            pipeline(
              d.readStream(),
              fs.createWriteStream('/tmp/test'),
              (err) => {
                if (err) {
                  done(err);
                } else {
                  assert.ok(nock.isDone());
                  const {size} = fs.statSync('/tmp/test');
                  assert.deepStrictEqual(size, bytes);
                  assert.deepStrictEqual(active, 0);
                  assert.deepStrictEqual(activeMax, 4);
                  assert.deepStrictEqual([1, 2, 3, 4, 5], downloadingPartNos);
                  assert.deepStrictEqual([1, 4, 2, 5, 3], downloadedPartNos);
                  assert.deepStrictEqual([1, 2, 3, 4, 5], donePartNos);
                  done();
                }
              }
            );
          });
          it('number of parts > concurrency', (done) => {
            const bytes = 41000000;
            nockRange(0, 7999999, bytes, 100);
            nockRange(8000000, 15999999, bytes, 500);
            nockRange(16000000, 23999999, bytes, 400);
            nockRange(24000000, 31999999, bytes, 200);
            nockRange(32000000, 39999999, bytes, 300);
            nockRange(40000000, 40999999, bytes, 100);
            mockfs({
              '/tmp': {
              }
            });
            const d = download({bucket:'bucket', key: 'key', version: 'version'}, {partSizeInMegabytes: 8, concurrency: 4});
            let active = 0;
            let activeMax = 0;
            const downloadingPartNos = [];
            const downloadedPartNos = [];
            const donePartNos = [];
            d.on('part:downloading', ({partNo}) => {
              active++;
              activeMax = Math.max(activeMax, active);
              downloadingPartNos.push(partNo);
            });
            d.on('part:downloaded', ({partNo}) => {
              downloadedPartNos.push(partNo);
            });
            d.on('part:done', ({partNo}) => {
              active--;
              donePartNos.push(partNo);
            });
            pipeline(
              d.readStream(),
              fs.createWriteStream('/tmp/test'),
              (err) => {
                if (err) {
                  done(err);
                } else {
                  assert.ok(nock.isDone());
                  const {size} = fs.statSync('/tmp/test');
                  assert.deepStrictEqual(size, bytes);
                  assert.deepStrictEqual(active, 0);
                  assert.deepStrictEqual(activeMax, 4);
                  assert.deepStrictEqual([1, 2, 3, 4, 5, 6], downloadingPartNos);
                  assert.deepStrictEqual([1, 4, 5, 3, 2, 6], downloadedPartNos);
                  assert.deepStrictEqual([1, 2, 3, 4, 5, 6], donePartNos);
                  done();
                }
              }
            );
          });
        });
      });
      describe('S3 retries', () => {
        describe('timeout', () => {
          it('recover', (done) => {
            const bytes = 33000000;
            nockRange(0, 7999999, bytes);
            nockRange(8000000, 15999999, bytes);
            nock('https://bucket.s3.eu-west-1.amazonaws.com', {
              reqheaders: {
                range: 'bytes=16000000-23999999',
                'x-amz-content-sha256': /.*/,
                'x-amz-date': /.*/,
                authorization: /.*/
              }
            })
              .get('/key')
              .query({
                versionId: 'version'
              })
              .times(4)
              .delayConnection(200)
              .reply(206, Buffer.alloc(8000000), {
                'Content-Length': '8000000',
                'Content-Range': `bytes 16000000-23999999/${bytes}`
              });
            nockRange(16000000, 23999999, bytes);
            nockRange(24000000, 31999999, bytes);
            nockRange(32000000, 32999999, bytes);
            mockfs({
              '/tmp': {
              }
            });
            const d = download({bucket:'bucket', key: 'key', version: 'version'}, {partSizeInMegabytes: 8, concurrency: 4, connectionTimeoutInMilliseconds: 100});
            pipeline(
              d.readStream(),
              fs.createWriteStream('/tmp/test'),
              (err) => {
                if (err) {
                  done(err);
                } else {
                  assert.ok(nock.isDone());
                  const {size} = fs.statSync('/tmp/test');
                  assert.deepStrictEqual(size, bytes);
                  done();
                }
              }
            );
          });
          it('too many retries', (done) => {
            const bytes = 33000000;
            nockRange(0, 7999999, bytes);
            nockRange(8000000, 15999999, bytes);
            nock('https://bucket.s3.eu-west-1.amazonaws.com', {
              reqheaders: {
                range: 'bytes=16000000-23999999',
                'x-amz-content-sha256': /.*/,
                'x-amz-date': /.*/,
                authorization: /.*/
              }
            })
              .get('/key')
              .query({
                versionId: 'version'
              })
              .times(5)
              .delayConnection(200)
              .reply(206, Buffer.alloc(8000000), {
                'Content-Length': '8000000',
                'Content-Range': `bytes 16000000-23999999/${bytes}`
              });
            nockRange(24000000, 31999999, bytes);
            nockRange(32000000, 32999999, bytes);
            mockfs({
              '/tmp': {
              }
            });
            const d = download({bucket:'bucket', key: 'key', version: 'version'}, {partSizeInMegabytes: 8, concurrency: 4, connectionTimeoutInMilliseconds: 100});
            pipeline(
              d.readStream(),
              fs.createWriteStream('/tmp/test'),
              (err) => {
                if (err) {
                  assert.ok(nock.isDone());
                  assert.deepStrictEqual(err.code, 'ECONNRESET');
                  done();
                } else {
                  assert.fail();
                }
              }
            );
          });
        });
        describe('ECONNRESET', () => {
          it('recover', (done) => {
            const bytes = 33000000;
            nockRange(0, 7999999, bytes);
            nockRange(8000000, 15999999, bytes);
            nock('https://bucket.s3.eu-west-1.amazonaws.com', {
              reqheaders: {
                range: 'bytes=16000000-23999999',
                'x-amz-content-sha256': /.*/,
                'x-amz-date': /.*/,
                authorization: /.*/
              }
            })
              .get('/key')
              .query({
                versionId: 'version'
              })
              .times(4)
              .replyWithError({code: 'ECONNRESET'});
            nockRange(16000000, 23999999, bytes);
            nockRange(24000000, 31999999, bytes);
            nockRange(32000000, 32999999, bytes);
            mockfs({
              '/tmp': {
              }
            });
            const d = download({bucket:'bucket', key: 'key', version: 'version'}, {partSizeInMegabytes: 8, concurrency: 4});
            pipeline(
              d.readStream(),
              fs.createWriteStream('/tmp/test'),
              (err) => {
                if (err) {
                  done(err);
                } else {
                  assert.ok(nock.isDone());
                  const {size} = fs.statSync('/tmp/test');
                  assert.deepStrictEqual(size, bytes);
                  done();
                }
              }
            );
          });
          it('too many retries', (done) => {
            const bytes = 33000000;
            nockRange(0, 7999999, bytes);
            nockRange(8000000, 15999999, bytes);
            nock('https://bucket.s3.eu-west-1.amazonaws.com', {
              reqheaders: {
                range: 'bytes=16000000-23999999',
                'x-amz-content-sha256': /.*/,
                'x-amz-date': /.*/,
                authorization: /.*/
              }
            })
              .get('/key')
              .query({
                versionId: 'version'
              })
              .times(5)
              .replyWithError({code: 'ECONNRESET'});
            nockRange(24000000, 31999999, bytes);
            nockRange(32000000, 32999999, bytes);
            mockfs({
              '/tmp': {
              }
            });
            const d = download({bucket:'bucket', key: 'key', version: 'version'}, {partSizeInMegabytes: 8, concurrency: 4});
            pipeline(
              d.readStream(),
              fs.createWriteStream('/tmp/test'),
              (err) => {
                if (err) {
                  assert.ok(nock.isDone());
                  assert.deepStrictEqual(err.code, 'ECONNRESET');
                  done();
                } else {
                  assert.fail();
                }
              }
            );
          });
        });
        describe('5XX', () => {
          it('recover', (done) => {
            const bytes = 33000000;
            nockRange(0, 7999999, bytes);
            nockRange(8000000, 15999999, bytes);
            nock('https://bucket.s3.eu-west-1.amazonaws.com', {
              reqheaders: {
                range: 'bytes=16000000-23999999',
                'x-amz-content-sha256': /.*/,
                'x-amz-date': /.*/,
                authorization: /.*/
              }
            })
              .get('/key')
              .query({
                versionId: 'version'
              })
              .times(4)
              .reply(500);
            nockRange(16000000, 23999999, bytes);
            nockRange(24000000, 31999999, bytes);
            nockRange(32000000, 32999999, bytes);
            mockfs({
              '/tmp': {
              }
            });
            const d = download({bucket:'bucket', key: 'key', version: 'version'}, {partSizeInMegabytes: 8, concurrency: 4});
            pipeline(
              d.readStream(),
              fs.createWriteStream('/tmp/test'),
              (err) => {
                if (err) {
                  done(err);
                } else {
                  assert.ok(nock.isDone());
                  const {size} = fs.statSync('/tmp/test');
                  assert.deepStrictEqual(size, bytes);
                  done();
                }
              }
            );
          });
          it('too many retries', (done) => {
            const bytes = 33000000;
            nockRange(0, 7999999, bytes);
            nockRange(8000000, 15999999, bytes);
            nock('https://bucket.s3.eu-west-1.amazonaws.com', {
              reqheaders: {
                range: 'bytes=16000000-23999999',
                'x-amz-content-sha256': /.*/,
                'x-amz-date': /.*/,
                authorization: /.*/
              }
            })
              .get('/key')
              .query({
                versionId: 'version'
              })
              .times(5)
              .reply(500);
            nockRange(24000000, 31999999, bytes);
            nockRange(32000000, 32999999, bytes);
            mockfs({
              '/tmp': {
              }
            });
            const d = download({bucket:'bucket', key: 'key', version: 'version'}, {partSizeInMegabytes: 8, concurrency: 4});
            pipeline(
              d.readStream(),
              fs.createWriteStream('/tmp/test'),
              (err) => {
                if (err) {
                  assert.ok(nock.isDone());
                  assert.deepStrictEqual(err.message, 'status code: 500, content-type: undefined');
                  done();
                } else {
                  assert.fail();
                }
              }
            );
          });
        });
      });
    });
    describe('file', () => {
      it('happy', (done) => {
        const bytes = 1000000;
        nockPart(1000000, 1, 1, bytes);
        mockfs({
          '/tmp': {
          }
        });
        download({bucket:'bucket', key: 'key', version: 'version'}, {concurrency: 4}).file('/tmp/test', (err) => {
          if (err) {
            done(err);
          } else {
            assert.ok(nock.isDone());
            const {size} = fs.statSync('/tmp/test');
            assert.deepStrictEqual(size, bytes);
            done();
          }
        });
      });
      it('abort', (done) => {
        const bytes = 1000000;
        nockPart(1000000, 1, 1, bytes, 200);
        mockfs({
          '/tmp': {
          }
        });
        const d = download({bucket:'bucket', key: 'key', version: 'version'}, {concurrency: 4});
        setTimeout(() => {
          d.abort();
        }, 100);
        d.file('/tmp/test', (err) => {
          if (err) {
            assert.ok(nock.isDone());
            assert.deepStrictEqual(err.code, 's3-getobject-accelerator:aborted');
            done();
          } else {
            assert.fail();
          }
        });
      });
    });
  });
  describe('credentials via IMDS', () => {
    before(() => {
      nock.disableNetConnect();
    });
    after(() => {
      nock.enableNetConnect();
    });
    beforeEach(() => {
      nockImds();
    });
    afterEach(() => {
      mockfs.restore();
      nock.cleanAll();
      clearCache();
    });
    describe('readStream', () => {
      it('happy', (done) => {
        const bytes = 8000000;
        nockRange(0, 7999999, bytes);
        mockfs({
          '/tmp': {
          }
        });
        pipeline(
          download({bucket:'bucket', key: 'key', version: 'version'}, {partSizeInMegabytes: 8, concurrency: 4}).readStream(),
          fs.createWriteStream('/tmp/test'),
          (err) => {
            if (err) {
              done(err);
            } else {
              assert.ok(nock.isDone());
              const {size} = fs.statSync('/tmp/test');
              assert.deepStrictEqual(size, bytes);
              done();
            }
          }
        );
      });
    });
  });
});

