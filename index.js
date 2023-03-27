const { PassThrough } = require('node:stream');
const { EventEmitter } = require('node:events');

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

exports.download = (s3, {bucket, key, version}, {partSizeInMegabytes, concurrency, waitForWriteBeforeDownloladingNextPart}) => {
  const emitter = new EventEmitter();
  const partSizeInBytes = (partSizeInMegabytes > 0) ? partSizeInMegabytes*1000000 : null;
  const stream = new PassThrough();

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
    const req = s3.getObject(params);
    partsDownloading[partNo] = req;
    req.send((err, data) => {
      delete partsDownloading[partNo];
      if (err) {
        cb(err);
      } else {
        cb(null, data);
      }
    });
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
    s3.getObject(params, (err, data) => {
      if (err) {
        stream.destroy(err);
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
            });
          }
        } else {
          const contentRange = parseContentRange(data.ContentRange);
          if (contentRange === undefined) {
            stream.destroy(new Error('unexpected content range'));
          } else {
            emitter.emit('part:downloaded', {partNo: 1});
            bytesToDownload = contentRange.length;
            if (bytesToDownload <= partSizeInBytes) {
              stream.end(data.Body, () => {
                emitter.emit('part:done', {partNo: 1});
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

  return {
    readStream: () => {
      if (started === false)  {
        start();
      }
      return stream;
    },
    partsDownloading: () => Object.keys(partsDownloading).length,
    addListener: (eventName, listener) => emitter.addListener(eventName, listener),
    off: (eventName, listener) => emitter.off(eventName, listener),
    on: (eventName, listener) => emitter.on(eventName, listener),
    once: (eventName, listener) => emitter.once(eventName, listener),
    removeListener: (eventName, listener) => emitter.removeListener(eventName, listener)
  };
};
