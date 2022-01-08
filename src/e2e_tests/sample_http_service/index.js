const express = require('express')
const responseTime = require('response-time')

const app = express()
const port = 3001

let counter = 0;

app.use(responseTime(function (req, res, time) {
  var stat = (req.method + req.url).toLowerCase()
    .replace(/[:.]/g, '')
    .replace(/\//g, '_')
  console.log(stat, time)
}))

app.get('/counter', (req, res) => {
  res.send({ counter })
})

app.get('/bump_counter', (req, res) => {
  counter++
  res.send({ "result": [] })
})

app.get('/invalid', (req, res) => {
  res.send(500)
})

app.listen(port, () => {
  console.log(`counter app listening at http://localhost:${port}`)
})

