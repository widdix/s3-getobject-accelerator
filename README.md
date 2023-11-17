# S3 GetObject Accelerator

Get large objects from S3 by using parallel byte-range fetches/parts without the AWS SDK to improve performance.

> We measured a troughoput of 6.5 Gbit/s on an m5zn.6xlarge in eu-west-1 using this lib with this settings: `{concurrency: 64}`.

## Installation

```bash
npm i s3-getobject-accelerator
```

## Examples

### Compact

```js
const {createWriteStream} = require('node:fs');
const {pipeline} = require('node:stream');
const {download} = require('s3-getobject-accelerator');

pipeline(
  download({bucket: 'bucket', key: 'key', version: 'optional version'}, {partSizeInMegabytes: 8, concurrency: 4}).readStream(),
  createWriteStream('/tmp/test'),
  (err) => {
    if (err) {
      console.error('something went wrong', err);
    } else {
      console.log('done');
    }
  }
);
```

### More verbose

Get insights into the part downloads and write to file directly without stream if it is smaller than 1 TiB:

```js
const {createWriteStream} = require('node:fs');
const {download} = require('s3-getobject-accelerator');

const d = download({bucket: 'bucket', key: 'key', version: 'optional version'}, {partSizeInMegabytes: 8, concurrency: 4});

d.on('part:downloading', ({partNo}) => {
  console.log('start downloading part', partNo);
});
d.on('part:downloaded', ({partNo}) => {
  console.log('part downloaded, write to disk next in correct order', partNo);
});
d.on('part:writing', ({partNo}) => {
  console.log('start writing part to disk', partNo);
});
d.on('part:done', ({partNo}) => {
  console.log('part written to disk', partNo);
});

d.meta((err, metadata) => {
  if (err) {
    console.error('something went wrong', err);
  } else {
    if (metadata.objectLengthInBytes > 1024 * 1024 * 1024 * 1024) {
      console.error('file is larger than 1 TiB');
    } else {
      d.file('/tmp/test', (err) => {
        if (err) {
          console.error('something went wrong', err);
        } else {
          console.log('done');
        }
      });
    }
  }
});
```

## API

### download(s3source, options)

* `s3source` `<Object>`
  * `bucket` `<string>`
  * `key` `<string>`
  * `version` `<string>` (optional)
* `options` `<Object>`
  * `partSizeInMegabytes` `<number>` (optional)
  * `concurrency` `<number>`
  * `connectionTimeoutInMilliseconds` `<number>`
* Returns:
  * `meta(cb)` `<Function>` Get meta-data before starting the download (downloads the first part and keeps the body in memory until download starts)
    * `cb(err, metadata)` `<Function>`
      * `err` `<Error>`
      * `metadata` `<Object>`
        * `objectLengthInBytes` `<number>` Total size of object  (not just the part) in bytes
        * `parts` `<number>` (optional) Number of parts available
        * `objectLockMode` `<string>` The Object Lock mode currently in place for this object. (Valid Values: GOVERNANCE, COMPLIANCE)
        * `objectLockRetainUntilDate` `<Date>` The date and time when this object's Object Lock will expire.
        * `objectLockLegalHoldStatus` `<string>` Indicates whether this object has an active legal hold. This field is only returned if you have permission to view an object's legal hold status. (Valid Values: ON, OFF)
        * `lengthInBytes` `<number>` Total size of object (not just the part) in bytes (deprecated, use `objectLengthInBytes` instead)
  * `readStream()` `<Function>` Start download
    * Returns: [ReadStream](https://nodejs.org/api/stream.html#class-streamreadable)
  * `file(path, cb)` `<Function>` Start download
    * `path` `<string>`
    * `cb(err)` `<Function>`
      * `err` `<Error>`
  * `abort([err])` `<Function>` Abort download
    * `err` `<Error>`
  * `partsDownloading()` `<Function>` Number of parts downloading at the moment
    * Returns `<number>`
  * `addListener(eventName, listener)` See https://nodejs.org/api/events.html#emitteraddlistenereventname-listener
  * `off(eventName, listener)` See https://nodejs.org/api/events.html#emitteroffeventname-listener
  * `on(eventName, listener)` See https://nodejs.org/api/events.html#emitteroneventname-listener
  * `once(eventName, listener)` See https://nodejs.org/api/events.html#emitteronceeventname-listener
  * `removeListener(eventName, listener)` See https://nodejs.org/api/events.html#emitterremovelistenereventname-listener 

## AWS credentials / region

AWS credentials & region is fetched in the following order:

1. Environment variables
  * `AWS_REGION`
  * `AWS_ACCESS_KEY_ID`
  * `AWS_SECRET_ACCESS_KEY`
  * `AWS_SESSION_TOKEN` (optional)
2. IMDSv2

## Considerations

* Typical sizes `partSizeInMegabytes` are 8 MB or 16 MB. If objects are uploaded using a multipart upload, it’s a good practice to download them in the same part sizes ( do not specify `partSizeInMegabytes`), or at least aligned to part boundaries, for best performance (see https://docs.aws.amazon.com/whitepapers/latest/s3-optimizing-performance-best-practices/use-byte-range-fetches.html).
* Keep in mind that you pay per GET request to Amazon S3. The smaller the parts, the more expensive a download is.
