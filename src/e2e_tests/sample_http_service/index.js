const express = require('express')
const app = express()
const port = 3001

let counter = 0;

app.get('/counter', (req, res) => {
  res.send({ counter })
})

app.get('/bump_counter', (req, res) => {
  counter++
  res.send({ "tasks": [] })
})

app.get('/invalid', (req, res) => {
  res.send(500)
})

app.listen(port, () => {
  console.log(`counter app listening at http://localhost:${port}`)
})

