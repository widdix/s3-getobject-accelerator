const assert = require('assert');
const {pipeline} = require('node:stream');
const fs = require('node:fs');
const mockfs = require('mock-fs');
const {download} = require('../index.js');

function s3mock(mock) {
  return {
    getObject: (params, cb) => {
      if (cb === undefined) {
        return {
          send: (cb) => {
            mock(params, cb);
          },
          abort: () => {}
        };
      } else {
        mock(params, cb);
        return {};
      }
    }
  };
}

describe('handler', () => {
  afterEach(() => {
    mockfs.restore();
  });
  describe('without partSizeInMegabytes', () => {
    describe('one part', () => {
      it('download error', (done) => {
        const s3 = {
          getObject: (params, cb) => {
            cb(new Error('download error'));
          }
        };
        mockfs({
          '/tmp': {
          }
        });
        pipeline(
          download(s3, {bucket:'bucket', key: 'key', version: 'version'}, {concurrency: 4}).readStream(),
          fs.createWriteStream('/tmp/test'),
          (err) => {
            if (err) {
              assert.deepStrictEqual(err.message, 'download error');
              done();
            } else {
              assert.fail();
            }
          }
        );
      });
      it('happy', (done) => {
        const bytes = 1000000;
        const s3 = {
          getObject: (params, cb) => {
            cb(null, {
              Body: Buffer.alloc(bytes),
              ContentLength: bytes,
              ContentRange: `bytes 0-999999/${bytes}`
            });
          }
        };
        mockfs({
          '/tmp': {
          }
        });
        pipeline(
          download(s3, {bucket:'bucket', key: 'key', version: 'version'}, {concurrency: 4}).readStream(),
          fs.createWriteStream('/tmp/test'),
          (err) => {
            if (err) {
              done(err);
            } else {
              const {size} = fs.statSync('/tmp/test');
              assert.deepStrictEqual(size, bytes);
              done();
            }
          }
        );
      });
    });
    describe('multiple parts', () => {
      // FIXME it('download error', (done) => {});
      it('number of parts < concurrency', (done) => {
        const bytes = 17000000;
        const s3 = s3mock((params, cb) => {
          if (params.PartNumber === 1) {
            setTimeout(() => {
              cb(null, {
                Body: Buffer.alloc(8000000),
                ContentLength: 8000000,
                PartsCount: 3
              });
            }, 100);
          } else if (params.PartNumber === 2) {
            setTimeout(() => {
              cb(null, {
                Body: Buffer.alloc(8000000),
                ContentLength: 8000000,
                PartsCount: 3
              });
            }, 100);
          } else if (params.PartNumber === 3) {
            setTimeout(() => {
              cb(null, {
                Body: Buffer.alloc(1000000),
                ContentLength: 1000000,
                PartsCount: 3
              });
            }, 200);
          } else {
            cb(new Error(`unexpected part: ${params.PartNumber}`));
          }
        });
        mockfs({
          '/tmp': {
          }
        });
        const d = download(s3, {bucket:'bucket', key: 'key', version: 'version'}, {concurrency: 4, waitForWriteBeforeDownloladingNextPart: true});
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
        const s3 = s3mock((params, cb) => {
          if (params.PartNumber === 1) {
            setTimeout(() => {
              cb(null, {
                Body: Buffer.alloc(8000000),
                ContentLength: 8000000,
                PartsCount: 5
              });
            }, 100);
          } else if (params.PartNumber === 2) {
            setTimeout(() => {
              cb(null, {
                Body: Buffer.alloc(8000000),
                ContentLength: 8000000,
                PartsCount: 5
              });
            }, 200);
          } else if (params.PartNumber === 3) {
            setTimeout(() => {
              cb(null, {
                Body: Buffer.alloc(8000000),
                ContentLength: 8000000,
                PartsCount: 5
              });
            }, 400);
          } else if (params.PartNumber === 4) {
            setTimeout(() => {
              cb(null, {
                Body: Buffer.alloc(8000000),
                ContentLength: 8000000,
                PartsCount: 5
              });
            }, 100);
          } else if (params.PartNumber === 5) {
            setTimeout(() => {
              cb(null, {
                Body: Buffer.alloc(1000000),
                ContentLength: 1000000,
                PartsCount: 5
              });
            }, 300);
          } else {
            cb(new Error(`unexpected part: ${params.PartNumber}`));
          }
        });
        mockfs({
          '/tmp': {
          }
        });
        const d = download(s3, {bucket:'bucket', key: 'key', version: 'version'}, {concurrency: 4, waitForWriteBeforeDownloladingNextPart: true});
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
        const s3 = s3mock((params, cb) => {
          if (params.PartNumber === 1) {
            setTimeout(() => {
              cb(null, {
                Body: Buffer.alloc(8000000),
                ContentLength: 8000000,
                PartsCount: 6
              });
            }, 100);
          } else if (params.PartNumber === 2) {
            setTimeout(() => {
              cb(null, {
                Body: Buffer.alloc(8000000),
                ContentLength: 8000000,
                PartsCount: 6
              });
            }, 500);
          } else if (params.PartNumber === 3) {
            setTimeout(() => {
              cb(null, {
                Body: Buffer.alloc(8000000),
                ContentLength: 8000000,
                PartsCount: 6
              });
            }, 400);
          } else if (params.PartNumber === 4) {
            setTimeout(() => {
              cb(null, {
                Body: Buffer.alloc(8000000),
                ContentLength: 8000000,
                PartsCount: 6
              });
            }, 200);
          } else if (params.PartNumber === 5) {
            setTimeout(() => {
              cb(null, {
                Body: Buffer.alloc(8000000),
                ContentLength: 8000000,
                PartsCount: 6
              });
            }, 300);
          } else if (params.PartNumber === 6) {
            setTimeout(() => {
              cb(null, {
                Body: Buffer.alloc(1000000),
                ContentLength: 1000000,
                PartsCount: 6
              });
            }, 100);
          } else {
            cb(new Error(`unexpected part: ${params.PartNumber}`));
          }
        });
        mockfs({
          '/tmp': {
          }
        });
        const d = download(s3, {bucket:'bucket', key: 'key', version: 'version'}, {concurrency: 4, waitForWriteBeforeDownloladingNextPart: true});
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
        const s3 = {
          getObject: (params, cb) => {
            cb(new Error('download error'));
          }
        };
        mockfs({
          '/tmp': {
          }
        });
        pipeline(
          download(s3, {bucket:'bucket', key: 'key', version: 'version'}, {partSizeInMegabytes: 8, concurrency: 4}).readStream(),
          fs.createWriteStream('/tmp/test'),
          (err) => {
            if (err) {
              assert.deepStrictEqual(err.message, 'download error');
              done();
            } else {
              assert.fail();
            }
          }
        );
      });
      it('happy', (done) => {
        const bytes = 1000000;
        const s3 = {
          getObject: (params, cb) => {
            cb(null, {
              Body: Buffer.alloc(bytes),
              ContentLength: bytes,
              ContentRange: `bytes 0-999999/${bytes}`
            });
          }
        };
        mockfs({
          '/tmp': {
          }
        });
        pipeline(
          download(s3, {bucket:'bucket', key: 'key', version: 'version'}, {partSizeInMegabytes: 8, concurrency: 4}).readStream(),
          fs.createWriteStream('/tmp/test'),
          (err) => {
            if (err) {
              done(err);
            } else {
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
      const s3 = s3mock((params, cb) => {
        cb(null, {
          Body: Buffer.alloc(bytes),
          ContentLength: bytes,
          ContentRange: `bytes 0-7999999/${bytes}`
        });
      });
      mockfs({
        '/tmp': {
        }
      });
      pipeline(
        download(s3, {bucket:'bucket', key: 'key', version: 'version'}, {partSizeInMegabytes: 8, concurrency: 4}).readStream(),
        fs.createWriteStream('/tmp/test'),
        (err) => {
          if (err) {
            done(err);
          } else {
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
        const s3 = s3mock((params, cb) => {
          if (params.Range === 'bytes=0-7999999') { // part 1 (to get size of object)
            cb(null, {
              Body: Buffer.alloc(8000000),
              ContentLength: 8000000,
              ContentRange: `bytes 0-7999999/${bytes}`
            });
          } else if (params.Range === 'bytes=8000000-15999999') { // part 2
            cb(null, {
              Body: Buffer.alloc(8000000),
              ContentLength: 8000000,
              ContentRange: `bytes 8000000-15999999/${bytes}`
            });
          } else if (params.Range === 'bytes=16000000-23999999') { // part 3
            cb(null, {
              Body: Buffer.alloc(8000000),
              ContentLength: 8000000,
              ContentRange: `bytes 16000000-23999999/${bytes}`
            });
          } else {
            cb(new Error(`unexpected range: ${params.Range}`));
          }
        });
        mockfs({
          '/tmp': {
          }
        });
        pipeline(
          download(s3, {bucket:'bucket', key: 'key', version: 'version'}, {partSizeInMegabytes: 8, concurrency: 4}).readStream(),
          fs.createWriteStream('/tmp/test'),
          (err) => {
            if (err) {
              done(err);
            } else {
              const {size} = fs.statSync('/tmp/test');
              assert.deepStrictEqual(size, bytes);
              done();
            }
          }
        );
      });
      it('download error', (done) => {
        const bytes = 24000000;
        const s3 = s3mock((params, cb) => {
          if (params.Range === 'bytes=0-7999999') { // part 1 (to get size of object)
            cb(null, {
              Body: Buffer.alloc(8000000),
              ContentLength: 8000000,
              ContentRange: `bytes 0-7999999/${bytes}`
            });
          } else if (params.Range === 'bytes=8000000-15999999') { // part 2
            cb(new Error('download error'));
          } else if (params.Range === 'bytes=16000000-23999999') { // part 3
            cb(null, {
              Body: Buffer.alloc(8000000),
              ContentLength: 8000000,
              ContentRange: `bytes 16000000-23999999/${bytes}`
            });
          } else {
            cb(new Error(`unexpected range: ${params.Range}`));
          }
        });
        mockfs({
          '/tmp': {
          }
        });
        pipeline(
          download(s3, {bucket:'bucket', key: 'key', version: 'version'}, {partSizeInMegabytes: 8, concurrency: 4}).readStream(),
          fs.createWriteStream('/tmp/test'),
          (err) => {
            if (err) {
              assert.deepStrictEqual(err.message, 'download error');
              done();
            } else {
              assert.fail();
            }
          }
        );
      });
      it('last part size < part size', (done) => {
        const bytes = 17000000;
        const s3 = s3mock((params, cb) => {
          if (params.Range === 'bytes=0-7999999') { // part 1 (to get size of object)
            cb(null, {
              Body: Buffer.alloc(8000000),
              ContentLength: 8000000,
              ContentRange: `bytes 0-7999999/${bytes}`
            });
          } else if (params.Range === 'bytes=8000000-15999999') { // part 2
            cb(null, {
              Body: Buffer.alloc(8000000),
              ContentLength: 8000000,
              ContentRange: `bytes 8000000-15999999/${bytes}`
            });
          } else if (params.Range === 'bytes=16000000-16999999') { // part 3
            cb(null, {
              Body: Buffer.alloc(1000000),
              ContentLength: 1000000,
              ContentRange: `bytes 16000000-16999999/${bytes}`
            });
          } else {
            cb(new Error(`unexpected range: ${params.Range}`));
          }
        });
        mockfs({
          '/tmp': {
          }
        });
        pipeline(
          download(s3, {bucket:'bucket', key: 'key', version: 'version'}, {partSizeInMegabytes: 8, concurrency: 4}).readStream(),
          fs.createWriteStream('/tmp/test'),
          (err) => {
            if (err) {
              done(err);
            } else {
              const {size} = fs.statSync('/tmp/test');
              assert.deepStrictEqual(size, bytes);
              done();
            }
          }
        );
      });
      it('number of parts < concurrency', (done) => {
        const bytes = 17000000;
        const s3 = s3mock((params, cb) => {
          if (params.Range === 'bytes=0-7999999') { // part 1 (to get size of object)
            setTimeout(() => {
              cb(null, {
                Body: Buffer.alloc(8000000),
                ContentLength: 8000000,
                ContentRange: `bytes 0-7999999/${bytes}`
              });
            }, 100);
          } else if (params.Range === 'bytes=8000000-15999999') { // part 2
            setTimeout(() => {
              cb(null, {
                Body: Buffer.alloc(8000000),
                ContentLength: 8000000,
                ContentRange: `bytes 8000000-15999999/${bytes}`
              });
            }, 100);
          } else if (params.Range === 'bytes=16000000-16999999') { // part 3
            setTimeout(() => {
              cb(null, {
                Body: Buffer.alloc(1000000),
                ContentLength: 1000000,
                ContentRange: `bytes 16000000-16999999/${bytes}`
              });
            }, 200);
          } else {
            cb(new Error(`unexpected range: ${params.Range}`));
          }
        });
        mockfs({
          '/tmp': {
          }
        });
        const d = download(s3, {bucket:'bucket', key: 'key', version: 'version'}, {partSizeInMegabytes: 8, concurrency: 4, waitForWriteBeforeDownloladingNextPart: true});
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
        const s3 = s3mock((params, cb) => {
          if (params.Range === 'bytes=0-7999999') { // part 1 (to get size of object)
            setTimeout(() => {
              cb(null, {
                Body: Buffer.alloc(8000000),
                ContentLength: 8000000,
                ContentRange: `bytes 0-7999999/${bytes}`
              });
            }, 100);
          } else if (params.Range === 'bytes=8000000-15999999') { // part 2
            setTimeout(() => {
              cb(null, {
                Body: Buffer.alloc(8000000),
                ContentLength: 8000000,
                ContentRange: `bytes 8000000-15999999/${bytes}`
              });
            }, 200);
          } else if (params.Range === 'bytes=16000000-23999999') { // part 3
            setTimeout(() => {
              cb(null, {
                Body: Buffer.alloc(8000000),
                ContentLength: 8000000,
                ContentRange: `bytes 16000000-23999999/${bytes}`
              });
            }, 400);
          } else if (params.Range === 'bytes=24000000-31999999') { // part 4
            setTimeout(() => {
              cb(null, {
                Body: Buffer.alloc(8000000),
                ContentLength: 8000000,
                ContentRange: `bytes 24000000-31999999/${bytes}`
              });
            }, 100);
          } else if (params.Range === 'bytes=32000000-32999999') { // part 5
            setTimeout(() => {
              cb(null, {
                Body: Buffer.alloc(1000000),
                ContentLength: 1000000,
                ContentRange: `bytes 32000000-32999999/${bytes}`
              });
            }, 300);
          } else {
            cb(new Error(`unexpected range: ${params.Range}`));
          }
        });
        mockfs({
          '/tmp': {
          }
        });
        const d = download(s3, {bucket:'bucket', key: 'key', version: 'version'}, {partSizeInMegabytes: 8, concurrency: 4, waitForWriteBeforeDownloladingNextPart: true});
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
        const s3 = s3mock((params, cb) => {
          if (params.Range === 'bytes=0-7999999') { // part 1 (to get size of object)
            setTimeout(() => {
              cb(null, {
                Body: Buffer.alloc(8000000),
                ContentLength: 8000000,
                ContentRange: `bytes 0-7999999/${bytes}`
              });
            }, 100);
          } else if (params.Range === 'bytes=8000000-15999999') { // part 2
            setTimeout(() => {
              cb(null, {
                Body: Buffer.alloc(8000000),
                ContentLength: 8000000,
                ContentRange: `bytes 8000000-15999999/${bytes}`
              });
            }, 500);
          } else if (params.Range === 'bytes=16000000-23999999') { // part 3
            setTimeout(() => {
              cb(null, {
                Body: Buffer.alloc(8000000),
                ContentLength: 8000000,
                ContentRange: `bytes 16000000-23999999/${bytes}`
              });
            }, 400);
          } else if (params.Range === 'bytes=24000000-31999999') { // part 4
            setTimeout(() => {
              cb(null, {
                Body: Buffer.alloc(8000000),
                ContentLength: 8000000,
                ContentRange: `bytes 24000000-31999999/${bytes}`
              });
            }, 200);
          } else if (params.Range === 'bytes=32000000-39999999') { // part 5
            setTimeout(() => {
              cb(null, {
                Body: Buffer.alloc(8000000),
                ContentLength: 8000000,
                ContentRange: `bytes 32000000-39999999/${bytes}`
              });
            }, 300);
          } else if (params.Range === 'bytes=40000000-40999999') { // part 6
            setTimeout(() => {
              cb(null, {
                Body: Buffer.alloc(1000000),
                ContentLength: 1000000,
                ContentRange: `bytes 40000000-40999999/${bytes}`
              });
            }, 100);
          } else {
            cb(new Error(`unexpected range: ${params.Range}`));
          }
        });
        mockfs({
          '/tmp': {
          }
        });
        const d = download(s3, {bucket:'bucket', key: 'key', version: 'version'}, {partSizeInMegabytes: 8, concurrency: 4, waitForWriteBeforeDownloladingNextPart: true});
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

