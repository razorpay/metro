def parser = new groovy.json.JsonSlurper()
def json = parser.parseText(context.request.body)
if (json.deliveryAttempt < 2) {
    respond()
    .withStatusCode(500)
    .withContent(json.data)
} else{
    respond().withContent(json.data)
}
    

