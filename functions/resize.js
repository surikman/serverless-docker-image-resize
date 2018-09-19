const stream = require('stream')
// const AWS = require('aws-sdk')

// X-Ray SDK does not work with the node8 async pattern (only works with callbacks)
const awsXRay = require('aws-xray-sdk')
const AWS = awsXRay.captureAWS(require('aws-sdk'))
AWS.config.update({region: process.env.REGION})

const S3 = new AWS.S3({
  signatureVersion: 'v4'
})
const sharp = require('sharp')
const BUCKET = process.env.BUCKET
const URL = `http://${process.env.BUCKET}.s3-website.${process.env.REGION}.amazonaws.com`

// create the write stream abstraction for uploading data to S3
const writeStreamToS3 = ({ Bucket, Key }) => {
  const pass = new stream.PassThrough()
  return {
    writeStream: pass,
    uploadFinished: S3.upload({
      Body: pass,
      Bucket,
      ContentType: 'image/png',
      Key
    }).promise()
  }
}

// traces
const startStreamTrace = ({ val, newKey }) => {
  awsXRay.captureFunc('resizeStarted', function (subsegment) {
    subsegment.addAnnotation('New Key', newKey)
    subsegment.addAnnotation('Value', val)
  })
}
const endStreamTrace = ({ val, newKey }) => {
  awsXRay.captureFunc('resizeEnded', function (subsegment) {
    subsegment.addAnnotation('New Key', newKey)
    subsegment.addAnnotation('Value', val)
  })
}

const handler = async (event) => {
  const key = event.queryStringParameters.key
  const match = key.match(/(\d+)x(\d+)\/(.*)/)
  const width = parseInt(match[1], 10)
  const height = parseInt(match[2], 10)
  const originalKey = match[3]
  const newKey = '' + width + 'x' + height + '/' + originalKey

  try {
    // location of the resized image
    const resizedImageLocation = `${URL}/${newKey}`

    // create the read stream from S3
    const readStream = S3.getObject({ Bucket: BUCKET, Key: originalKey }).createReadStream()
    readStream.on('end', () => startStreamTrace(newKey))

    // create sharp resize stream
    const resize = sharp()
      .resize(width, height)
      .toFormat('png')
      .on('pipe', (val) => startStreamTrace({ val, key }))
      .on('end', (val) => endStreamTrace({ val, key }))

    // create the write stream to S3
    const { writeStream, uploadFinished } = writeStreamToS3({ Bucket: BUCKET, Key: newKey })

    // trigger the stream
    readStream
      .pipe(resize)
      .pipe(writeStream)

    // wait for the stream to finish
    const uploadedData = await uploadFinished
    console.log({
      ...uploadedData,
      BucketEndpoint: URL,
      ImageURL: resizedImageLocation
    }) // log data to Dashbird

    // return a 301 redirect to the newly created resource in S3
    return {
      statusCode: 301,
      headers: { 'location': resizedImageLocation },
      body: ''
    }
  } catch (err) {
    console.error(err)
    return Promise.reject(err)
  }
}

exports.handler = (event, context, callback) => {
  handler(event)
    .then(res => callback(null, res))
    .catch(err => callback(err, {
      statusCode: 500,
      body: err.message
    }))
}
