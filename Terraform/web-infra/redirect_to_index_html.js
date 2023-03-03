function handler(event) {
    var request = event.request;
    var uri = request.uri;

    if (uri.endsWith('/')) {
        // If the URL has a slash at the end, it's fine to just
        // modify its URI here:
        request.uri += 'index.html';
        return request;
    } else if (!uri.includes('.')) {
        // But if it doesn't have the slash, we need to redirect
        // so that the client browser will resolve relative URLs
        // correctly
        var response = {
            statusCode: 302,
            statusDescription: 'Found',
            headers:
                { "location": { "value": request.uri + '/index.html' } }
            }
        return response;
    } else {
        return request;
    }
}