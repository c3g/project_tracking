// Starts Swagger UI on /docs, with the specification URL given by the page (no inline script, see the CSP)
window.addEventListener("load", function () {
  var root = document.getElementById("swagger-ui");
  window.ui = SwaggerUIBundle({
    url: root.dataset.specUrl,
    dom_id: "#swagger-ui",
    deepLinking: true,
    tryItOutEnabled: false,
    // Session cookies go with Try it out requests, as with any request from this origin
    requestInterceptor: function (request) {
      request.credentials = "same-origin";
      return request;
    }
  });
});
