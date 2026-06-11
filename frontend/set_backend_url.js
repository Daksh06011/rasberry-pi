// Runtime backend URL placeholder
// Dynamically check if running locally or on production
if (window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1' || !window.location.hostname) {
    window.__BACKEND_URL = 'http://127.0.0.1:5000';
} else {
    window.__BACKEND_URL = window.location.origin;
}

console.info('[INIT] backend URL set to', window.__BACKEND_URL);
