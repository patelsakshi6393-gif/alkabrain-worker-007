var e;

try {
  importScripts('/js/shared.js', '/js/background.js');
} catch (error) {
  e = error;
  console.error(e);
}
