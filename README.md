# S3 Managed Download

Get large objects from S3 by using parallel byte-range fetches to improve performance.

Install:
```bash
npm i s3-managed-download
```

Use:
```js
const AWS = require('aws-sdk');
const {pipeline} = require('node:stream');
const {download} = require('s3-managed-download');

const s3 = new AWS.S3({apiVersion: '2006-03-01'});

pipeline(
  download(s3, {bucket: 'bucket', key: 'key', version: 'version'}, {partSizeInMegabytes: 8, concurrency: 4}).readStream(),
  fs.createWriteStream('/tmp/test'),
  (err) => {
    if (err) {
      console.error('something went wrong', err);
    } else {
      console.log('done');
    }
  }
);
```
