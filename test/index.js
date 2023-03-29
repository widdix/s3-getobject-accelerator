const assert = require('assert');
const {pipeline} = require('node:stream');
const fs = require('node:fs');
const mockfs = require('mock-fs');
const nock = require('nock');
const {download} = require('../index.js');

function nockPart(partSize, partNumber, parts, optionalTimeout) {
  const headers = {
    'Content-Length': `${partSize}`
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

describe('handler', () => {
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
  });
  describe('without partSizeInMegabytes', () => {
    describe('one part', () => {
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
          .reply(500);
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
              assert.deepStrictEqual(err.message, 'unexpected status code');
              done();
            } else {
              assert.fail();
            }
          }
        );
      });
      it('happy', (done) => {
        const bytes = 1000000;
        nockPart(1000000, 1, 1);
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
    });
    describe('multiple parts', () => {
      it('download error', (done) => {
        nockPart(8000000, 1, 3);
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
          .reply(500);
        nockPart(1000000, 3, 3);
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
              assert.deepStrictEqual(err.message, 'unexpected status code');
              done();
            } else {
              assert.fail();
            }
          }
        );
      });
      it('number of parts < concurrency', (done) => {
        const bytes = 17000000;
        nockPart(8000000, 1, 3);
        nockPart(8000000, 2, 3);
        nockPart(1000000, 3, 3);
        mockfs({
          '/tmp': {
          }
        });
        const d = download({bucket:'bucket', key: 'key', version: 'version'}, {concurrency: 4, waitForWriteBeforeDownloladingNextPart: true});
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
        nockPart(8000000, 1, 5, 100);
        nockPart(8000000, 2, 5, 200);
        nockPart(8000000, 3, 5, 400);
        nockPart(8000000, 4, 5, 100);
        nockPart(1000000, 5, 5, 300);
        mockfs({
          '/tmp': {
          }
        });
        const d = download({bucket:'bucket', key: 'key', version: 'version'}, {concurrency: 4, waitForWriteBeforeDownloladingNextPart: true});
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
        nockPart(8000000, 1, 6, 100);
        nockPart(8000000, 2, 6, 500);
        nockPart(8000000, 3, 6, 400);
        nockPart(8000000, 4, 6, 200);
        nockPart(8000000, 5, 6, 300);
        nockPart(1000000, 6, 6, 100);
        mockfs({
          '/tmp': {
          }
        });
        const d = download({bucket:'bucket', key: 'key', version: 'version'}, {concurrency: 4, waitForWriteBeforeDownloladingNextPart: true});
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
          .reply(500);
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
              assert.deepStrictEqual(err.message, 'unexpected status code');
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
          .reply(500);
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
              assert.deepStrictEqual(err.message, 'unexpected status code');
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
        const d = download({bucket:'bucket', key: 'key', version: 'version'}, {partSizeInMegabytes: 8, concurrency: 4, waitForWriteBeforeDownloladingNextPart: true});
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
        const d = download({bucket:'bucket', key: 'key', version: 'version'}, {partSizeInMegabytes: 8, concurrency: 4, waitForWriteBeforeDownloladingNextPart: true});
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
        const d = download({bucket:'bucket', key: 'key', version: 'version'}, {partSizeInMegabytes: 8, concurrency: 4, waitForWriteBeforeDownloladingNextPart: true});
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
});

