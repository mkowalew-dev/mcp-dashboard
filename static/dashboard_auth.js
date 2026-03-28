(function () {
  var orig = window.fetch;
  window.fetch = function () {
    var args = arguments;
    return orig.apply(this, args).then(function (r) {
      if (r.status === 401) {
        var path = window.location.pathname || "/";
        window.location.href = "/login?next=" + encodeURIComponent(path);
      }
      return r;
    });
  };
})();
