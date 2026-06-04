// Enhanced Dashboard JavaScript - All Features Combined
// Preserves all existing functionality + adds new enhanced features

// ========================
// GLOBAL VARIABLES & API CONFIG
// ========================

// API Configuration for decoupled frontend
// Use an injected runtime value `window.__BACKEND_URL` when available (Vercel can inject at build),
// fall back to localhost during development, otherwise use relative paths so Vercel can proxy `/api`.
const API_BASE_URL = window.__BACKEND_URL || (window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1' || !window.location.hostname ? 'http://127.0.0.1:5000' : 'https://reasonable-wonder-production-c57e.up.railway.app');

function parseLocalTime(t) {
    if (!t) return null;
    if (t instanceof Date) return t;
    try {
        let s = typeof t === 'string' ? t.replace('T', ' ').replace('Z', '') : String(t);
        if (s.includes('+')) {
            s = s.split('+')[0];
        }
        const parts = s.split(' ');
        const dateParts = parts[0].split('-');
        if (dateParts.length !== 3) return new Date(t);
        
        const timeParts = parts[1] ? parts[1].split(':') : [0, 0, 0];
        
        const year = parseInt(dateParts[0], 10);
        const month = parseInt(dateParts[1], 10) - 1;
        const day = parseInt(dateParts[2], 10);
        
        const hour = parseInt(timeParts[0], 10);
        const minute = parseInt(timeParts[1], 10);
        const second = parseFloat(timeParts[2] || 0);
        
        const d = new Date(year, month, day, hour, minute, Math.floor(second));
        return isNaN(d.getTime()) ? null : d;
    } catch (e) {
        console.warn('Failed to parse local time, falling back:', t, e);
        const d = new Date(t);
        return isNaN(d.getTime()) ? null : d;
    }
}

function setChartDayBounds(chart, targetDateObj) {
    if (!chart) return;
    const targetDate = targetDateObj ? new Date(targetDateObj) : new Date();
    const startOfDay = new Date(targetDate);
    startOfDay.setHours(0, 0, 0, 0);
    const endOfDay = new Date(targetDate);
    endOfDay.setHours(23, 59, 59, 999);
    
    let isZoomed = false;
    try {
        if (typeof chart.isZoomedOrPanned === 'function') {
            isZoomed = chart.isZoomedOrPanned();
        }
    } catch (e) {}
    
    if (!isZoomed) {
        if (!chart.options.scales) chart.options.scales = {};
        if (!chart.options.scales.x) chart.options.scales.x = {};
        chart.options.scales.x.min = startOfDay;
        chart.options.scales.x.max = endOfDay;
    }
}

/**
 * Wrapper for fetch that automatically adds API_BASE_URL and Authorization headers
 */
async function apiFetch(endpoint, options = {}) {
    const url = endpoint.startsWith('http') ? endpoint : `${API_BASE_URL}${endpoint}`;
    
    // Add auth header
    const token = localStorage.getItem('access_token');
    if (token) {
        options.headers = {
            ...options.headers,
            'Authorization': `Bearer ${token}`
        };
    }

    try {
        const response = await fetch(url, options);
        if (response.status === 401) {
            // Unauthorized - clear token and redirect to login
            localStorage.removeItem('access_token');
            localStorage.removeItem('user_data');
            window.location.href = 'login.html';
            return null;
        }
        return response;
    } catch (error) {
        console.error('API Fetch error:', error);
        throw error;
    }
}

// Existing variables (preserved)
let currentAvgWindow = 15;
let currentDeviceRoom = null;

// New enhanced variables
let currentDeviceId = null;
let currentDeviceType = 'basic'; // 'basic' or 'extended'
let socket = null;

// MQTT browser client variables (for direct broker connection fallback)
let mqttClient = null;
let mqttEnabled = false;
const MQTT_CONFIG = {
    broker: localStorage.getItem('mqtt_broker') || 'wss://fb1f89b92da34734b1ca59ef89f2dbfa.s1.eu.hivemq.cloud:8884/mqtt',
    topic: localStorage.getItem('mqtt_topic') || 'xiao/dashboard',
    username: localStorage.getItem('mqtt_username') || 'Daksh',
    password: localStorage.getItem('mqtt_password') || 'Sgn@1234',
    clientId: `browser-client-${Date.now()}`,
    reconnectPeriod: 4000,
    connectTimeout: 4000
};

// Ensure Chart.js Zoom plugin is registered when loaded via CDN
try {
  if (typeof Chart !== 'undefined' && typeof Chart.register === 'function') {
    const zoomPlugin = window.ChartZoom || window['chartjs-plugin-zoom'];
    if (zoomPlugin) {
      Chart.register(zoomPlugin);
    }
  }
} catch (e) {
  console.warn('Chart.js zoom plugin registration skipped:', e);
}

// ========================
// MQTT BROWSER CLIENT (Fallback for direct broker access)
// ========================

/**
 * Parse MQTT payload in both compact and extended formats
 * Compact: {"i":"xiao_001", "e":[temp,humid,press...], "pm":[pm1,pm2.5,...], "g":[lat,lon,...], "t":"..."}
 * Extended: {"deviceid":"xiao_001", "Temperature_C":..., "PM_data":{...}, "GPS":{...}}
 */
function parseMQTTPayload(payload) {
    try {
        let data = typeof payload === 'string' ? JSON.parse(payload) : payload;
        
        const timestamp = new Date(data.ts || data.t || data.timestamp || new Date()).toISOString();
        const deviceId = data.site || data.i || data.deviceid || 'unknown';
        
        let result = {
            device_id: deviceId,
            site: data.site,
            mac: data.mac,
            ip: data.ip,
            rssi: data.rssi,
            tsi_status: data.tsi,
            tsi_serial: data.tsi_serial,
            sensor: {
                timestamp: timestamp
            },
            extended: data.extended ? data.extended : {},
            raw_data: data  // Keep raw data for dynamic display
        };
        
        // Handle TSI format: {"site":"xiao-cam-01", "tsi_pm1":7, "tsi_pm25":7, "tsi_temp":30.2, "tsi_rh":72}
        if (data.tsi_pm1 !== undefined || data.tsi_temp !== undefined) {
            console.log('📡 Parsing TSI MQTT format');
            
            result.extended = {
                temperature_c: data.tsi_temp,
                humidity_percent: data.tsi_rh,
                pressure_hpa: data.tsi_pressure,
                no2_ppb: data.no2,
                voc_ppb: data.voc,
                sound_db: data.sound
            };
            
            result.sensor = {
                timestamp: timestamp,
                pm1: data.tsi_pm1,
                pm2_5: data.tsi_pm25,
                pm4: data.tsi_pm4,
                pm10: data.tsi_pm10
            };
            
            if (data.lat && data.lon) {
                result.extended.gps_lat = data.lat;
                result.extended.gps_lon = data.lon;
            }
        }
        // Handle compact format: {"i":"...", "e":[...], "pm":[...], "g":[...]}
        else if (data.e && Array.isArray(data.e) && data.pm && Array.isArray(data.pm)) {
            console.log('📡 Parsing compact MQTT format');
            
            result.extended = {
                temperature_c: data.e[0],
                humidity_percent: data.e[1],
                pressure_hpa: data.e[2],
                uv_index: data.e[3],
                lux: data.e[4],
                voc_ppb: data.e[5],
                no2_ppb: data.e[6],
                noise_db: data.e[7]
            };
            
            result.sensor = {
                timestamp: timestamp,
                pm1: data.pm[0],
                pm2_5: data.pm[1],
                pm4: data.pm[2],
                pm10: data.pm[3],
                tsp: data.pm[4]
            };
            
            if (data.g && Array.isArray(data.g)) {
                result.extended.gps_lat = data.g[0];
                result.extended.gps_lon = data.g[1];
                result.extended.gps_alt_m = data.g[2];
                result.extended.gps_speed_kmh = data.g[3];
            }
        } 
        // Handle extended format: {"deviceid":"...", "Temperature_C":..., "PM_data":{...}}
        else if (data.Temperature_C !== undefined || (data.PM_data && typeof data.PM_data === 'object')) {
            console.log('📡 Parsing extended MQTT format');
            
            result.extended = {
                temperature_c: data.Temperature_C,
                humidity_percent: data['Humidity_%'],
                pressure_hpa: data.Pressure_hPa,
                uv_index: data.UV_Index,
                lux: data.Lux,
                voc_ppb: data.VOC_ppb,
                no2_ppb: data.NO2_ppb,
                noise_db: data.Noise_dB
            };
            
            const pm = data.PM_data || {};
            result.sensor = {
                timestamp: timestamp,
                pm1: pm.PM1,
                pm2_5: pm.PM2_5,
                pm4: pm.PM4,
                pm10: pm.PM10,
                tsp: pm.TSP_um
            };
            
            const gps = data.GPS || {};
            if (gps.Latitude) {
                result.extended.gps_lat = gps.Latitude;
                result.extended.gps_lon = gps.Longitude;
                result.extended.gps_alt_m = gps['Altitude_m'];
                result.extended.gps_speed_kmh = gps.Speed_kmh;
            }
        }
        // Handle final Waveshare format: {"site":"...", "location":{...}, "sky":{...}, "ads1115":{...}}
        else if (data.ads1115 && data.sky) {
            console.log('📡 Parsing final Waveshare MQTT format');
            
            let noise = null;
            let no2 = null;
            let voc = null;
            
            const channels = data.ads1115.channels || [];
            channels.forEach(channel => {
                const name = channel.name;
                const val = channel.raw !== undefined ? channel.raw : channel.value;
                if (name === 'SOUND') noise = val;
                else if (name === 'NO2') no2 = val;
                else if (name === 'VOC') voc = val;
            });
            
            result.extended = {
                temperature_c: data.temperature || data.temp || (data.tsi && data.tsi.temp),
                humidity_percent: data.humidity || data.rh || (data.tsi && data.tsi.humidity),
                pressure_hpa: data.pressure,
                voc_ppb: voc,
                no2_ppb: no2,
                noise_db: noise,
                lux: data.sky.lux,
                cloud_cover_percent: data.sky.cloud_cover
            };
            
            const pmRoot = data.PM_data || {};
            const pmTsi = data.tsi || {};
            result.sensor = {
                timestamp: timestamp,
                pm1: data.pm1 || pmRoot.PM1 || pmTsi.pm1,
                pm2_5: data.pm2_5 || pmRoot.PM2_5 || pmTsi.pm25 || pmTsi.pm2_5,
                pm4: data.pm4 || pmRoot.PM4 || pmTsi.pm4,
                pm10: data.pm10 || pmRoot.PM10 || pmTsi.pm10,
                tsp: data.tsp || pmRoot.TSP_um || pmTsi.tsp
            };
        }
        // Handle simple PM-only format
        else {
            console.log('📡 Parsing simple MQTT format (PM data only)');
            
            result.sensor = {
                timestamp: timestamp,
                pm1: data.pm1 || data.PM1,
                pm2_5: data.pm2_5 || data.PM2_5,
                pm4: data.pm4 || data.PM4,
                pm10: data.pm10 || data.PM10,
                tsp: data.tsp || data.TSP_um
            };
        }
        
        console.log('✅ Parsed MQTT data:', result);
        return result;
        
    } catch (error) {
        console.error('❌ Error parsing MQTT payload:', error);
        return null;
    }
}

/**
 * Initialize MQTT browser client for direct broker connection (WebSocket)
 * Used as fallback when Socket.IO is unavailable
 */
function initializeMQTTClient() {
    // Check if mqtt.js library is available
    if (typeof mqtt === 'undefined') {
        console.warn('🚫 mqtt.js library not loaded. Cannot initialize MQTT browser client.');
        console.info('💡 Add <script src="https://unpkg.com/mqtt/dist/mqtt.min.js"></script> to templates to enable');
        return false;
    }
    
    try {
        console.log('🔌 Initializing MQTT browser client...');
        console.log(`   Broker: ${MQTT_CONFIG.broker}`);
        console.log(`   Topic: ${MQTT_CONFIG.topic}`);
        
        // Create MQTT client
        mqttClient = mqtt.connect(MQTT_CONFIG.broker, {
            clientId: MQTT_CONFIG.clientId,
            username: MQTT_CONFIG.username || undefined,
            password: MQTT_CONFIG.password || undefined,
            reconnectPeriod: MQTT_CONFIG.reconnectPeriod,
            connectTimeout: MQTT_CONFIG.connectTimeout,
            resubscribe: true,
            clean: true
        });
        
        // Connection handlers
        mqttClient.on('connect', function() {
            console.log('✅ MQTT browser client connected');
            mqttEnabled = true;
            updateConnectionStatus(true);
            
            // Subscribe to device topic
            mqttClient.subscribe(MQTT_CONFIG.topic, { qos: 1 }, (err) => {
                if (err) {
                    console.error('❌ MQTT subscription error:', err);
                } else {
                    console.log(`📡 Subscribed to MQTT topic: ${MQTT_CONFIG.topic}`);
                }
            });
        });
        
        mqttClient.on('message', function(topic, message) {
            try {
                console.log(`📨 MQTT message received on ${topic}`);
                
                const payload = message.toString();
                const data = parseMQTTPayload(payload);
                
                console.log('📊 Raw MQTT data:', data);
                
                // Always display the MQTT data received
                if (data) {
                    console.log('✅ Displaying MQTT data');
                    displayDynamicMQTTData(data);
                    
                    // Also process if device matches or if no device is selected
                    if (!currentDeviceId || String(data.device_id) === String(currentDeviceId)) {
                        console.log('🎯 Processing data for dashboard...');
                        processIncomingData(data);
                    } else {
                        console.log('⏭️  Different device, but still displaying data');
                    }
                }
            } catch (error) {
                console.error('❌ Error processing MQTT message:', error);
            }
        });
        
        mqttClient.on('error', function(error) {
            console.error('❌ MQTT client error:', error);
            updateConnectionStatus(false);
        });
        
        mqttClient.on('disconnect', function() {
            console.warn('⚠️  MQTT client disconnected');
            mqttEnabled = false;
            updateConnectionStatus(false);
        });
        
        mqttClient.on('reconnect', function() {
            console.log('🔄 MQTT client reconnecting...');
        });
        
        return true;
        
    } catch (error) {
        console.error('❌ Failed to initialize MQTT client:', error);
        return false;
    }
}

/**
 * Disconnect MQTT client gracefully
 */
function disconnectMQTTClient() {
    if (mqttClient && mqttClient.connected) {
        console.log('🔌 Disconnecting MQTT client...');
        mqttClient.end();
        mqttEnabled = false;
    }
}

/**
 * Update MQTT broker configuration (for future settings page)
 */
function updateMQTTConfig(broker, topic, username, password) {
    localStorage.setItem('mqtt_broker', broker);
    localStorage.setItem('mqtt_topic', topic);
    localStorage.setItem('mqtt_username', username);
    localStorage.setItem('mqtt_password', password);
    
    // Reconnect with new config
    if (mqttClient && mqttClient.connected) {
        disconnectMQTTClient();
    }
    
    MQTT_CONFIG.broker = broker;
    MQTT_CONFIG.topic = topic;
    MQTT_CONFIG.username = username;
    MQTT_CONFIG.password = password;
    
    initializeMQTTClient();
}

function initializeWebSocket() {
    // Check if Socket.IO is available
    if (typeof io === 'undefined') {
        console.warn('Socket.IO not found');
        // Try MQTT first as fallback, then polling
        if (!initializeMQTTClient()) {
            console.warn('MQTT client init failed, using polling fallback');
            startRealtimePolling();
        }
        return;
    }

    try {
        // Get current protocol and host for Railway compatibility
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const host = window.location.host;

        // Initialize socket connection with Railway-compatible settings
        socket = io(API_BASE_URL, {
            transports: ['polling', 'websocket'], // Try polling first, then websocket
            reconnection: true,
            reconnectionAttempts: 3, // Reduce reconnection attempts
            reconnectionDelay: 2000, // Increase delay
            timeout: 10000, // Reduce timeout
            forceNew: false,
            upgrade: true,
            withCredentials: true // Send cookies for authentication
        });

        // Connection event handlers
        socket.on('connect', function() {
            console.log('✅ WebSocket connected to server');
            updateConnectionStatus(true);
            stopRealtimePolling(); // Stop polling when WebSocket connects

            // Join device room if a device is selected
            if (currentDeviceId) {
                joinDeviceRoom(currentDeviceId);
            }
        });

        socket.on('disconnect', function(reason) {
            console.log('❌ WebSocket disconnected:', reason);
            updateConnectionStatus(false);
            // Only start polling if it's a network issue, not intentional disconnect
            if (reason !== 'io client disconnect') {
                // Delay polling start to avoid immediate reloads
                setTimeout(() => {
                    startRealtimePolling();
                }, 5000);
            }
        });

        socket.on('connect_error', function(error) {
            console.error('❌ WebSocket connection error:', error);
            updateConnectionStatus(false);
            // Don't immediately start polling - let it retry WebSocket first
        });

        socket.on('reconnect', function(attemptNumber) {
            console.log('🔄 WebSocket reconnected after', attemptNumber, 'attempts');
            updateConnectionStatus(true);
            stopRealtimePolling();
            disconnectMQTTClient(); // Stop MQTT if WebSocket recovered
        });

        socket.on('reconnect_error', function(error) {
            console.error('❌ WebSocket reconnection failed:', error);
            // Try MQTT as fallback before polling
            if (!mqttEnabled && typeof mqtt !== 'undefined') {
                console.log('🔄 Attempting MQTT fallback after WebSocket failure...');
                initializeMQTTClient();
            }
            // Also start polling as additional fallback
            startRealtimePolling();
        });

        // Handle incoming data - prevent duplicate processing
        socket.on('new_data', function(data) {
            if (String(data.device_id) === String(currentDeviceId)) {
                console.log('📡 Received WebSocket sensor data');
                processWebSocketData(data);
                fetchData(24, false); // Trigger silent chart update with new real-time point
                
                // Real-time display name update from HiveMQ message!
                if (data.device_name) {
                    const deviceSelect = document.getElementById('deviceSelect');
                    if (deviceSelect) {
                        const option = Array.from(deviceSelect.options).find(o => String(o.value) === String(data.device_id));
                        if (option && option.textContent !== data.device_name) {
                            console.log(`🔄 Dynamically renaming dropdown option from ${option.textContent} to ${data.device_name}`);
                            option.textContent = data.device_name;
                            option.setAttribute('data-name', data.device_name);
                            // Also update header/panel if this is the active device
                            if (String(deviceSelect.value) === String(data.device_id)) {
                                updateDeviceInfoPanel(data.device_name, data.device_identifier || option.getAttribute('data-deviceid') || '', currentDeviceType, option.getAttribute('data-relay') === 'True');
                            }
                        }
                    }
                }
            }
        });

        socket.on('new_extended_data', function(data) {
            if (currentDeviceType === 'extended' && String(data.device_id) === String(currentDeviceId)) {
                console.log('📡 Received WebSocket extended data');
                // Normalize WebSocket data before passing to updateExtendedData
                const normalized = normalizeIncomingData({ extended: data });
                if (normalized && normalized.extended) {
                    updateExtendedData(normalized.extended);
                    fetchData(24, false); // Trigger silent chart update with new real-time point
                } else {
                    console.warn('❌ Failed to normalize WebSocket extended data');
                }
            }
        });

    } catch (error) {
        console.error('❌ WebSocket initialization failed:', error);
        startRealtimePolling();
    }
}

let map = null;
let deviceMarker = null;
let currentTheme = 'light';
// Locking axis behavior for PM time chart
let fixedYAxisMax = null;
// Rigid axes flag for all charts
let rigidAxesEnabled = true;
// Device selection map
let deviceSelectMap = null;
let deviceSelectMarkers = [];
let latestData = null;
// Polling fallback when websockets are not available or disconnected
let pollingIntervalId = null;
let autoUpdateIntervalId = null;
// Rigid maxima cache per chart
const rigidMaxByChart = {};

// Chart instances storage
let charts = {
    // Existing charts
    timeChart: null,
    // Extended device charts
    environmentalCombinedChart: null,
    mqttLiveChart: null,
    mqttValueChart: null,
    // Unified combined chart
    unifiedChart: null
};

window.charts = charts;

let mqttLiveSeries = {
    labels: [],
    temperature: [],
    humidity: [],
    pm1: [],
    pm25: [],
    pm4: [],
    pm10: []
};

// Color schemes for all parameters - Curated premium HSL-tailored colors
const colorScheme = {
    pm1: 'rgba(99, 102, 241, 0.85)',       // Indigo
    pm2_5: 'rgba(16, 185, 129, 0.85)',     // Emerald
    pm4: 'rgba(6, 182, 212, 0.85)',        // Cyan
    pm10: 'rgba(245, 158, 11, 0.85)',       // Amber
    tsp: 'rgba(139, 92, 246, 0.85)',       // Violet
    temperature: 'rgba(244, 63, 94, 0.85)', // Rose
    humidity: 'rgba(20, 184, 166, 0.85)',    // Teal
    pressure: 'rgba(14, 165, 233, 0.85)',    // Sky Blue
    voc: 'rgba(168, 85, 247, 0.85)',        // Purple
    no2: 'rgba(217, 70, 239, 0.85)',        // Pink
    noise: 'rgba(249, 115, 22, 0.85)',       // Coral/Orange
    speed: 'rgba(100, 116, 139, 0.85)',      // Slate
    cloud: 'rgba(156, 163, 175, 0.85)'       // Gray
};


// ========================
// THEME MANAGEMENT (Enhanced from your existing)
// ========================

const themeToggle = document.getElementById('theme-toggle');
themeToggle.addEventListener('change', function() {
    const theme = this.checked ? 'dark' : 'light';
    currentTheme = theme;
    document.documentElement.setAttribute('data-theme', theme);
    localStorage.setItem('theme', theme);
    updateChartsTheme();
});

// Initialize theme from localStorage (your existing logic preserved)
const savedTheme = localStorage.getItem('theme') || 'light';
currentTheme = savedTheme;
document.documentElement.setAttribute('data-theme', currentTheme);
themeToggle.checked = currentTheme === 'dark';

// Chart theme variables (your existing)
let chartGridColor = getComputedStyle(document.documentElement).getPropertyValue('--chart-grid').trim() || 'rgba(0, 0, 0, 0.1)';
let chartTextColor = getComputedStyle(document.documentElement).getPropertyValue('--text-color').trim() || '#212529';

// ========================
// INITIALIZATION (Enhanced)
// ========================

document.addEventListener('DOMContentLoaded', function() {
    // Initialize components with error handling
    try {
        initializeCharts();
        initializeExtendedCharts();
        startLivePulseLoop();
        initializeDeviceSelection();
        initializeWebSocket();
        initializeDeviceSelectionMap();
        setDefaultDates();
        initializeMapToggle();
        initializeDeviceQuickActions();
        setupRelayControls();
        initializeTemperatureDisplay();

        // Test data loading with a sample device ID for development
        console.log('Dashboard initialized. Please select a device to load data.');

        // Don't call updateQuickStats here as it requires data
        // updateQuickStats(); // Remove this line

    } catch (error) {
        console.error('Initialization error:', error);
        createAlert('Failed to initialize dashboard. Please refresh the page.', 'danger');
    }
});

// Extract temperature value from various payload formats
function extractTempValue(payloadText) {
    const data = payloadText.trim();
    const directValue = Number(data);

    if (Number.isFinite(directValue)) {
        return directValue;
    }

    try {
        const payload = JSON.parse(data);

        const candidates = [
            Array.isArray(payload?.e) ? payload.e[0] : undefined,
            payload?.Temperature_C,
            payload?.temperature_c,
            payload?.temperature,
            payload?.temp,
        ];

        for (const candidate of candidates) {
            if (candidate === undefined || candidate === null || candidate === "") {
                continue;
            }

            const tempNumber = Number(candidate);
            if (Number.isFinite(tempNumber)) {
                return tempNumber;
            }
        }
    } catch (error) {
        // Ignore non-JSON payloads
    }

    const parsed = Number(data);
    return Number.isFinite(parsed) ? parsed : null;
}

// Initialize temperature display with MQTT connection
function initializeTemperatureDisplay() {
    const tempElement = document.getElementById('currentTemp');
    if (!tempElement) return;

    // Set initial placeholder
    updateEnvironmentalCard('currentTemp', null, '°C');
    console.log('✅ Temperature display initialized');

    // Connect to MQTT broker
    try {
        if (typeof mqtt === 'undefined') {
            console.warn('⚠️ mqtt.js library not loaded');
            return;
        }

        const mqttClient = mqtt.connect('wss://fb1f89b92da34734b1ca59ef89f2dbfa.s1.eu.hivemq.cloud:8884/mqtt', {
            username: 'Daksh',
            password: 'Sgn@1234',
            clientId: `dashboard-temp-${Date.now()}`,
            reconnectPeriod: 4000,
            connectTimeout: 4000,
        });

        mqttClient.on('connect', () => {
            console.log('✅ Connected to HiveMQ broker');
            mqttClient.subscribe('xiao/dashboard', (err) => {
                if (err) {
                    console.error('❌ Failed to subscribe to xiao/dashboard:', err);
                } else {
                    console.log('✅ Subscribed to xiao/dashboard topic');
                }
            });
        });

        mqttClient.on('message', (topic, message) => {
            if (topic === 'xiao/dashboard') {
                const tempValue = extractTempValue(message.toString());
                console.log('📡 Received dashboard data:', tempValue, 'from', topic);
                updateEnvironmentalCard('currentTemp', tempValue, '°C');
            }
        });

        mqttClient.on('error', (error) => {
            console.error('❌ MQTT error:', error);
        });

        mqttClient.on('close', () => {
            console.log('⚠️ MQTT connection closed');
        });

    } catch (error) {
        console.error('❌ Error initializing MQTT:', error);
    }
}

// ========================
// DEVICE SELECTION (Enhanced)
// ========================

async function initializeDeviceSelection() {
    const deviceSelect = document.getElementById('deviceSelect');
    if (!deviceSelect) return;

    try {
        const response = await apiFetch('/api/user/devices');
        if (response) {
            const data = await response.json();
            if (data.success && data.devices) {
                // Clear existing except first
                while (deviceSelect.options.length > 1) {
                    deviceSelect.remove(1);
                }
                
                // Add fetched devices
                data.devices.forEach(device => {
                    const option = document.createElement('option');
                    option.value = device.id;
                    option.textContent = device.name || device.deviceid;
                    option.setAttribute('data-deviceid', device.deviceid);
                    option.setAttribute('data-name', device.name || '');
                    option.setAttribute('data-type', device.source_type === 'mqtt' ? 'extended' : 'basic');
                    option.setAttribute('data-relay', device.has_relay ? 'True' : 'False');
                    deviceSelect.appendChild(option);
                });

                // Update total devices count
                const totalDevicesEl = document.getElementById('totalDevices');
                if (totalDevicesEl) {
                    totalDevicesEl.textContent = data.devices.length;
                }

                // Update online devices count
                const onlineDevicesEl = document.getElementById('onlineDevices');
                if (onlineDevicesEl) {
                    const onlineCount = data.devices.filter(d => d.online).length;
                    onlineDevicesEl.textContent = onlineCount;
                }
            }
        }
    } catch (error) {
        console.error('Error fetching devices:', error);
    }

    // Trigger initial selection if a device is pre-selected
    if (deviceSelect && deviceSelect.value) {
        handleDeviceSelection();
    } else if (deviceSelect && deviceSelect.options.length > 1) {
        // Auto select first available device
        deviceSelect.selectedIndex = 1;
        handleDeviceSelection();
    }

    deviceSelect.addEventListener('change', handleDeviceSelection);
}

function handleDeviceSelection() {
    // Stop any existing auto-update timer
    stopAutoUpdateTimer();

    const deviceSelect = document.getElementById('deviceSelect');
    const selectedOption = deviceSelect.options[deviceSelect.selectedIndex];

    if (!selectedOption.value) {
        hideAllTabs();
        return;
    }

    currentDeviceId = selectedOption.value;
    currentDeviceType = selectedOption.getAttribute('data-type');
    const hasRelay = selectedOption.getAttribute('data-relay') === 'True';
    const deviceName = selectedOption.getAttribute('data-name');
    const deviceId = selectedOption.getAttribute('data-deviceid');

    console.log('=== DEVICE SELECTION START ===');
    console.log('Device selected:', {
        currentDeviceId,
        currentDeviceType,
        hasRelay,
        deviceName,
        deviceId,
        selectedOptionAttributes: {
            'data-type': selectedOption.getAttribute('data-type'),
            'data-relay': selectedOption.getAttribute('data-relay'),
            'data-name': selectedOption.getAttribute('data-name'),
            'data-deviceid': selectedOption.getAttribute('data-deviceid')
        }
    });

    // Update device info panel
    updateDeviceInfoPanel(deviceName, deviceId, currentDeviceType, hasRelay);

    // Show appropriate tabs
    console.log('Calling showDeviceTabs with:', currentDeviceType, hasRelay);
    showDeviceTabs(currentDeviceType, hasRelay);

    // Join WebSocket room
    joinDeviceRoom(currentDeviceId);

    // Load initial data
    console.log('Fetching initial data...');
    fetchData(24);

    // Start 10-second auto-update timer for real-time dashboard data refresh
    startAutoUpdateTimer();

    // Pan Google Map to the selected device's location
    if (deviceSelectMap && deviceSelectMarkers.length > 0) {
        const marker = deviceSelectMarkers.find(m => String(m.deviceId) === String(currentDeviceId));
        if (marker) {
            const pos = marker.getPosition();
            deviceSelectMap.panTo(pos);
            const lat = pos.lat();
            const lng = pos.lng();
            // Zoom out to level 4 if location is in Varanger/Barents Sea (70.0, 30.0) or default/ocean areas
            if ((Math.abs(lat - 70.0) < 1.0 && Math.abs(lng - 30.0) < 1.0) || (lat === 0.0 && lng === 0.0)) {
                deviceSelectMap.setZoom(4);
            } else {
                if (deviceSelectMap.getZoom() < 8) {
                    deviceSelectMap.setZoom(8);
                }
            }
            if (marker.infoWindow) {
                marker.infoWindow.open(deviceSelectMap, marker);
            }
        }
    }

    // Update device type indicator
    const indicator = document.getElementById('deviceTypeIndicator');
    if (indicator) {
        indicator.textContent = currentDeviceType.toUpperCase();
        indicator.className = `badge ${currentDeviceType === 'extended' ? 'bg-info' : 'bg-primary'} ms-2`;
        indicator.style.display = 'inline';
    }

    console.log('=== DEVICE SELECTION COMPLETE ===');
}


function getParameterLabel(param) {
    const labels = {
        'pm1': 'PM1 (μg/m³)',
        'pm2_5': 'PM2.5 (μg/m³)',
        'pm4': 'PM4 (μg/m³)',
        'pm10': 'PM10 (μg/m³)',
        'tsp': 'TSP (μg/m³)',
        'temperature_c': 'Temperature (°C)',
        'humidity_percent': 'Humidity (%)',
        'pressure_hpa': 'Pressure (hPa)',
        'voc_ppb': 'VOC (ppb)',
        'no2_ppb': 'NO₂_index',
        'noise_db': 'noise_level_index',
        'gps_speed_kmh': 'Speed (km/h)',
        'cloud_cover_percent': 'Cloud Cover (%)'
    };
    return labels[param] || param;
}

function joinDeviceRoom(deviceId) {
    if (socket && socket.connected) {
        // Parse user_id from local storage
        const userDataStr = localStorage.getItem('user_data');
        let userId = null;
        if (userDataStr) {
            try {
                const userData = JSON.parse(userDataStr);
                userId = userData.id;
            } catch (e) {
                console.error('Error parsing user data:', e);
            }
        }

        // Leave previous room
        if (currentDeviceRoom && currentDeviceRoom !== deviceId) {
            socket.emit('leave', { device_id: currentDeviceRoom, user_id: userId });
        }
        
        // Join new room
        socket.emit('join', { device_id: deviceId, user_id: userId });
        currentDeviceRoom = deviceId;
        console.log(`Joined room for device: ${deviceId}, user: ${userId}`);
    }
}

function getGeocodedAddress(lat, lon, callback) {
    let parsedLat = parseFloat(lat);
    let parsedLon = parseFloat(lon);
    if (isNaN(parsedLat) || isNaN(parsedLon)) {
        callback("Unknown Location");
        return;
    }
    // Check if coordinates are dummy and map them to Chandigarh
    if (Math.abs(parsedLat - 70.0) < 0.1 && Math.abs(parsedLon - 30.0) < 0.1) {
        parsedLat = 30.7333;
        parsedLon = 76.7794;
    } else if (Math.abs(parsedLat - 30.0) < 0.1 && Math.abs(parsedLon - 70.0) < 0.1) {
        parsedLat = 30.7433;
        parsedLon = 76.7894;
    }
    const latLng = { lat: parsedLat, lng: parsedLon };
    if (typeof google !== 'undefined' && google.maps && google.maps.Geocoder) {
        const geocoder = new google.maps.Geocoder();
        geocoder.geocode({ location: latLng }, (results, status) => {
            if (status === 'OK' && results[0]) {
                callback(results[0].formatted_address);
            } else {
                callback(`Coordinates: ${latLng.lat.toFixed(4)}, ${latLng.lng.toFixed(4)}`);
            }
        });
    } else {
        callback(`Coordinates: ${latLng.lat.toFixed(4)}, ${latLng.lng.toFixed(4)}`);
    }
}

// ========================
// DEVICE SELECTION MAP
// ========================
async function initializeDeviceSelectionMap() {
    const mapContainer = document.getElementById('deviceSelectMap');
    if (!mapContainer) return;

    if (!deviceSelectMap) {
        deviceSelectMap = new google.maps.Map(mapContainer, {
            center: { lat: 20.5937, lng: 78.9629 },
            zoom: 4,
            mapTypeControl: true,
            streetViewControl: false
        });
    }

    async function fetchLocations() {
        const res = await apiFetch('/api/device_locations');
        const json = await res.json();
        return json.devices || [];
    }

    function clearMarkers() {
        if (deviceSelectMarkers) {
            deviceSelectMarkers.forEach(m => m.setMap(null));
        }
        deviceSelectMarkers = [];
    }

    function addMarkers(devices) {
        clearMarkers();
        const bounds = new google.maps.LatLngBounds();
        let validLocations = 0;

        devices.forEach(d => {
            if (!Number.isFinite(d.gps_lat) || !Number.isFinite(d.gps_lon)) return;
            
            let lat = d.gps_lat;
            let lon = d.gps_lon;
            // Apply coordinates override for dummy coordinates
            if (Math.abs(lat - 70.0) < 0.1 && Math.abs(lon - 30.0) < 0.1) {
                lat = 30.7333;
                lon = 76.7794;
            } else if (Math.abs(lat - 30.0) < 0.1 && Math.abs(lon - 70.0) < 0.1) {
                lat = 30.7433;
                lon = 76.7894;
            }
            
            const latLng = { lat: lat, lng: lon };
            
            const marker = new google.maps.Marker({
                position: latLng,
                map: deviceSelectMap,
                title: d.name || d.deviceid
            });
            
            marker.deviceId = String(d.id);
            marker.deviceIdentifier = String(d.deviceid);
            marker.configuredLat = lat;
            marker.configuredLon = lon;

            const infowindow = new google.maps.InfoWindow({
                content: `
                    <div class="p-3 font-sans text-dark" style="max-width: 250px;">
                        <h6 class="mb-1 text-primary fw-bold">${d.name || d.deviceid}</h6>
                        <div class="small text-muted mb-2">Device ID: ${d.deviceid}</div>
                        <div class="d-flex align-items-center mb-1">
                            <i class="bi bi-geo-alt-fill text-danger me-2"></i>
                            <span class="small" id="marker-addr-${d.id}">Loading location...</span>
                        </div>
                        <div class="small text-secondary mt-2">
                            Last active: ${d.last_update ? new Date(d.last_update).toLocaleString() : 'Never'}
                        </div>
                    </div>
                `
            });
            
            marker.infoWindow = infowindow;

            marker.addListener('click', () => {
                infowindow.open(deviceSelectMap, marker);
                selectDeviceById(d.id);
                getGeocodedAddress(lat, lon, (addr) => {
                    const el = document.getElementById(`marker-addr-${d.id}`);
                    if (el) el.textContent = addr;
                });
            });

            deviceSelectMarkers.push(marker);
            bounds.extend(latLng);
            validLocations++;

            // Preload address into the info window
            getGeocodedAddress(lat, lon, (addr) => {
                google.maps.event.addListenerOnce(infowindow, 'domready', () => {
                    const el = document.getElementById(`marker-addr-${d.id}`);
                    if (el) el.textContent = addr;
                });
            });
        });

        if (validLocations > 0) {
            // Auto focus and open the info window for the currently selected device if available
            if (currentDeviceId) {
                const activeMarker = deviceSelectMarkers.find(m => String(m.deviceId) === String(currentDeviceId));
                if (activeMarker) {
                    deviceSelectMap.panTo(activeMarker.getPosition());
                    if (activeMarker.infoWindow) {
                        activeMarker.infoWindow.open(deviceSelectMap, activeMarker);
                    }
                    const lat = activeMarker.getPosition().lat();
                    const lng = activeMarker.getPosition().lng();
                    if ((Math.abs(lat - 70.0) < 1.0 && Math.abs(lng - 30.0) < 1.0) || (lat === 0.0 && lng === 0.0)) {
                        deviceSelectMap.setZoom(4);
                    } else {
                        deviceSelectMap.setZoom(8);
                    }
                    return;
                }
            }

            deviceSelectMap.fitBounds(bounds);
            
            // Limit zoom level when bounds fit a single marker (prevent blank blue screen)
            if (validLocations === 1) {
                google.maps.event.addListenerOnce(deviceSelectMap, 'idle', () => {
                    const center = bounds.getCenter();
                    const lat = center.lat();
                    const lng = center.lng();
                    // Zoom out to level 4 if location is in Varanger/Barents Sea (70.0, 30.0) or default/ocean areas
                    if ((Math.abs(lat - 70.0) < 1.0 && Math.abs(lng - 30.0) < 1.0) || (lat === 0.0 && lng === 0.0)) {
                        deviceSelectMap.setZoom(4);
                    } else if (deviceSelectMap.getZoom() > 8) {
                        deviceSelectMap.setZoom(8);
                    }
                });
            }
        }
    }

    function selectDeviceById(id) {
        const deviceSelect = document.getElementById('deviceSelect');
        if (!deviceSelect) return;
        const option = Array.from(deviceSelect.options).find(o => String(o.value) === String(id));
        if (option) {
            deviceSelect.value = option.value;
            deviceSelect.dispatchEvent(new Event('change'));
        } else {
            createAlert('Device not found in selection list', 'warning');
        }
    }

    async function refreshMarkers() {
        try {
            const devices = await fetchLocations();
            addMarkers(devices);
        } catch (e) {
            console.error(e);
            createAlert('Failed to load device locations', 'danger');
        }
    }

    // Buttons
    const refreshBtn = document.getElementById('refreshDeviceMapBtn');
    const fitBtn = document.getElementById('fitDeviceMapBtn');
    if (refreshBtn) refreshBtn.addEventListener('click', refreshMarkers);
    if (fitBtn) fitBtn.addEventListener('click', () => {
        const bounds = new google.maps.LatLngBounds();
        deviceSelectMarkers.forEach(m => bounds.extend(m.getPosition()));
        if (deviceSelectMarkers.length) {
            deviceSelectMap.fitBounds(bounds);
            if (deviceSelectMarkers.length === 1) {
                google.maps.event.addListenerOnce(deviceSelectMap, 'idle', () => {
                    const center = bounds.getCenter();
                    const lat = center.lat();
                    const lng = center.lng();
                    if ((Math.abs(lat - 70.0) < 1.0 && Math.abs(lng - 30.0) < 1.0) || (lat === 0.0 && lng === 0.0)) {
                        deviceSelectMap.setZoom(4);
                    } else if (deviceSelectMap.getZoom() > 8) {
                        deviceSelectMap.setZoom(8);
                    }
                });
            }
        }
    });

    await refreshMarkers();
}

function updateGoogleMapLocation(deviceId, lat, lon, deviceName) {
    if (!deviceSelectMap) return;
    
    let parsedLat = parseFloat(lat);
    let parsedLon = parseFloat(lon);
    if (isNaN(parsedLat) || isNaN(parsedLon) || (parsedLat === 0 && parsedLon === 0)) return;
    
    // Find if marker already exists
    let marker = deviceSelectMarkers.find(m => String(m.deviceId) === String(deviceId));
    
    // Check if incoming coordinates are dummy coordinates
    const isDummy1 = (Math.abs(parsedLat - 70.0) < 0.1 && Math.abs(parsedLon - 30.0) < 0.1);
    const isDummy2 = (Math.abs(parsedLat - 30.0) < 0.1 && Math.abs(parsedLon - 70.0) < 0.1);
    
    if (isDummy1 || isDummy2) {
        if (marker && marker.configuredLat !== undefined && marker.configuredLon !== undefined) {
            // Use the already configured/resolved coordinates from the DB/backend
            parsedLat = marker.configuredLat;
            parsedLon = marker.configuredLon;
        } else {
            // Fallback to Chandigarh default overrides
            if (isDummy1) {
                parsedLat = 30.7333;
                parsedLon = 76.7794;
            } else {
                parsedLat = 30.7433;
                parsedLon = 76.7894;
            }
        }
    } else {
        // If they are real/new coordinates, update the marker's configured coordinates
        if (marker) {
            marker.configuredLat = parsedLat;
            marker.configuredLon = parsedLon;
        }
    }
    
    const latLng = { lat: parsedLat, lng: parsedLon };
    
    if (marker) {
        marker.setPosition(latLng);
        getGeocodedAddress(parsedLat, parsedLon, (addr) => {
            if (marker.infoWindow) {
                marker.infoWindow.setContent(`
                    <div class="p-3 font-sans text-dark" style="max-width: 250px;">
                        <h6 class="mb-1 text-primary fw-bold">${deviceName || marker.title}</h6>
                        <div class="small text-muted mb-2">Device ID: ${marker.deviceIdentifier || deviceId}</div>
                        <div class="d-flex align-items-center mb-1">
                            <i class="bi bi-geo-alt-fill text-danger me-2"></i>
                            <span class="small">${addr}</span>
                        </div>
                        <div class="small text-secondary mt-2">
                            Last active: ${new Date().toLocaleString()}
                        </div>
                    </div>
                `);
            }
        });
    } else {
        // Create new marker
        marker = new google.maps.Marker({
            position: latLng,
            map: deviceSelectMap,
            title: deviceName || `Device ${deviceId}`
        });
        marker.deviceId = String(deviceId);
        marker.configuredLat = parsedLat;
        marker.configuredLon = parsedLon;
        
        const infowindow = new google.maps.InfoWindow({
            content: `
                <div class="p-3 font-sans text-dark" style="max-width: 250px;">
                    <h6 class="mb-1 text-primary fw-bold">${deviceName || `Device ${deviceId}`}</h6>
                    <div class="small text-muted mb-2">Device ID: ${deviceId}</div>
                    <div class="d-flex align-items-center mb-1">
                        <i class="bi bi-geo-alt-fill text-danger me-2"></i>
                        <span class="small" id="marker-addr-${deviceId}">Loading location...</span>
                    </div>
                    <div class="small text-secondary mt-2">
                        Last active: ${new Date().toLocaleString()}
                    </div>
                </div>
            `
        });
        
        marker.infoWindow = infowindow;

        marker.addListener('click', () => {
            infowindow.open(deviceSelectMap, marker);
            selectDeviceById(deviceId);
            getGeocodedAddress(parsedLat, parsedLon, (addr) => {
                const el = document.getElementById(`marker-addr-${deviceId}`);
                if (el) el.textContent = addr;
            });
        });
        
        deviceSelectMarkers.push(marker);
        
        getGeocodedAddress(parsedLat, parsedLon, (addr) => {
            google.maps.event.addListenerOnce(infowindow, 'domready', () => {
                const el = document.getElementById(`marker-addr-${deviceId}`);
                if (el) el.textContent = addr;
            });
        });
    }
    
    // If the updated device is the currently selected device, pan/center map to it
    if (String(deviceId) === String(currentDeviceId)) {
        deviceSelectMap.panTo(latLng);
        // Ensure zoom is reasonable (between 5 and 8 is good for context)
        if ((Math.abs(latLng.lat - 70.0) < 1.0 && Math.abs(latLng.lng - 30.0) < 1.0) || (latLng.lat === 0.0 && latLng.lng === 0.0)) {
            deviceSelectMap.setZoom(4);
        } else {
            if (deviceSelectMap.getZoom() > 8) {
                // Keep zoom reasonable
            } else {
                deviceSelectMap.setZoom(8);
            }
        }
    }
}

function updateDeviceInfoPanel(name, deviceId, type, hasRelay) {
    const deviceInfo = document.getElementById('deviceInfo');
    const deviceStatus = document.getElementById('deviceStatus');
    const deviceType = document.getElementById('deviceType');
    const relayStatusRow = document.getElementById('relayStatusRow');

    if (deviceInfo) deviceInfo.style.display = 'block';
    if (deviceStatus) {
        deviceStatus.textContent = 'Online';
        deviceStatus.className = 'badge bg-success';
    }
    if (deviceType) {
        deviceType.textContent = type.toUpperCase();
        deviceType.className = `badge ${type === 'extended' ? 'bg-info' : 'bg-primary'}`;
    }

    if (relayStatusRow) {
        if (hasRelay) {
            relayStatusRow.style.display = 'block';
            const relayControls = document.getElementById('relayControls');
            if (relayControls) relayControls.style.display = 'block';
        } else {
            relayStatusRow.style.display = 'none';
            const relayControls = document.getElementById('relayControls');
            if (relayControls) relayControls.style.display = 'none';
        }
    }
}

// WebSocket data processing - for real-time updates only
function processWebSocketData(data) {
    try {
        if (!data) {
            console.warn('No WebSocket data received');
            return;
        }

        console.log('📡 Processing WebSocket data:', data);

        // WebSocket data is for real-time updates only
        // Don't replace chart history data, just update current readings

        // Update sensor readings if available
        if (data.sensor) {
            updatePMReadings(data.sensor);
            updateQuickStats({ sensor: data.sensor });
            updateAQI({ sensor: data.sensor });
            checkThresholds({
                sensor: data.sensor,
                status: data.status || { thresholds: latestData?.status?.thresholds }
            });
        }

        // Update connection status
        updateConnectionStatus(true);

        // Update last update time
        if (data.sensor && data.sensor.timestamp) {
            const lastUpdate = document.getElementById('lastUpdate');
            const readingsTime = document.getElementById('readingsTime');
            const lastUpdateTime = document.getElementById('lastUpdateTime');

            const timeStr = new Date(data.sensor.timestamp).toLocaleTimeString();

            if (lastUpdate) lastUpdate.textContent = new Date(data.sensor.timestamp).toLocaleString();
            if (readingsTime) readingsTime.textContent = timeStr;
            if (lastUpdateTime) lastUpdateTime.textContent = timeStr;
        }

        console.log('✅ WebSocket data processed successfully');

    } catch (error) {
        console.error('❌ Error processing WebSocket data:', error);
    }
}

function safeProcessIncomingData(data) {
    try {
        if (!data) {
            console.warn('No data received');
            return;
        }

        console.log('Processing incoming data:', data);

        // Display the dynamic MQTT data
        displayDynamicMQTTData(data);

        // Normalize the data
        const normalizedData = normalizeIncomingData(data);

        if (!normalizedData) {
            console.error('Failed to normalize data');
            return;
        }

        // Convert timestamps safely
        if (normalizedData.history && normalizedData.history.timestamps) {
            normalizedData.history.timestamps = normalizedData.history.timestamps.map(t => {
                try {
                    // Normalize: replace space with T for ISO 8601 compliance (SQLite format fix)
                    const normalized = typeof t === 'string' ? t.replace(' ', 'T') : t;
                    const d = new Date(normalized);
                    return isNaN(d.getTime()) ? new Date() : d;
                } catch (e) {
                    console.warn('Invalid timestamp:', t);
                    return new Date();
                }
            });
        }

        // Call all update functions
        latestData = normalizedData;
        updateDashboard(normalizedData);
        updateCharts(normalizedData);
        updateExtendedCharts(normalizedData);
        checkThresholds(normalizedData);
        calculateThresholdFrequency(normalizedData);
        updateQuickStats(normalizedData);
        updateAQI(normalizedData);

        // Handle extended data
        if (normalizedData.extended) {
            console.log('Calling updateExtendedData with:', normalizedData.extended);
            updateExtendedData(normalizedData.extended);
        }

    } catch (error) {
        console.error('Error processing incoming data:', error);
    }
}

function debugDataFlow(data, stage) {
    console.group(`Data Flow: ${stage}`);
    console.log('Raw data structure:', data);
    if (data && data.sensor) console.log('Sensor data:', data.sensor);
    if (data && data.extended) console.log('Extended data:', data.extended);
    if (data && data.history) console.log('History data keys:', Object.keys(data.history));
    console.groupEnd();
}

function processIncomingData(data) {
    // Call the safe processing function
    safeProcessIncomingData(data);
}

function showDeviceTabs(deviceType, hasRelay) {
    console.log('showDeviceTabs called for single-view dashboard:', { deviceType, hasRelay });

    const tabContent = document.getElementById('deviceTypeTabContent');
    if (tabContent) {
        tabContent.style.display = 'block';
    }
}

// Function to load environmental data cards from backend API
function loadEnvironmentalDataCards() {
    console.log('Loading environmental data cards...');

    if (!currentDeviceId) {
        console.warn('No device selected, cannot load environmental data cards');
        return;
    }

    // Fetch live data from backend API
    apiFetch(`/api/data?hours=1&deviceid=${currentDeviceId}`)
        .then(response => {
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            return response.json();
        })
        .then(data => {
            console.log('Received environmental data for cards:', data);

            // Extract extended data
            const extendedData = data.extended || {};
            const sensorData = data.sensor || {};

            // Create environmental data cards
            createEnvironmentalDataCards(extendedData, sensorData);
        })
        .catch(error => {
            console.error('Error loading environmental data cards:', error);
            createAlert('Failed to load environmental data cards', 'danger');
        });
}

// Function to create environmental data cards dynamically
function createEnvironmentalDataCards(extendedData, sensorData) {
    const container = document.getElementById('environmentalDataContainer');
    if (!container) {
        console.error('Environmental data container not found');
        return;
    }

    // Define the environmental parameters to display
    const environmentalParams = [
        {
            key: 'temperature_c',
            label: 'Temperature',
            icon: 'bi-thermometer-high',
            color: 'danger',
            unit: '°C',
            data: extendedData.temperature_c
        },
        {
            key: 'humidity_percent',
            label: 'Humidity',
            icon: 'bi-droplet-half',
            color: 'info',
            unit: '%',
            data: extendedData.humidity_percent
        },
        {
            key: 'pressure_hpa',
            label: 'Pressure',
            icon: 'bi-speedometer2',
            color: 'success',
            unit: ' hPa',
            data: extendedData.pressure_hpa
        },
        {
            key: 'voc_ppb',
            label: 'VOC',
            icon: 'bi-cloud-haze2',
            color: 'warning',
            unit: ' ppb',
            data: extendedData.voc_ppb
        },
        {
            key: 'no2_ppb',
            label: 'NO₂',
            icon: 'bi-cloud-haze2',
            color: 'warning',
            unit: '',
            data: extendedData.no2_ppb
        },
        {
            key: 'noise_db',
            label: 'Noise',
            icon: 'bi-volume-up',
            color: 'purple',
            unit: ' dB',
            data: extendedData.noise_db
        },
        {
            key: 'lux',
            label: 'Light Level',
            icon: 'bi-sun',
            color: 'warning',
            unit: ' lux',
            data: extendedData.lux
        },
        {
            key: 'uv_index',
            label: 'UV Index',
            icon: 'bi-sun-fill',
            color: 'orange',
            unit: '',
            data: extendedData.uv_index
        }
    ];

    const receivedParams = environmentalParams.filter(param => {
        return typeof param.data === 'number' && Number.isFinite(param.data);
    });

    if (receivedParams.length === 0) {
        container.innerHTML = `
            <div class="col-12">
                <div class="alert alert-secondary mb-0 text-center">
                    No environmental parameters received yet.
                </div>
            </div>
        `;
        return;
    }

    // Create HTML for cards
    const cardsHTML = receivedParams.map(param => {
        const value = param.data.toFixed(1);
        const valueColor = '#0d6efd';

        return `
            <div class="col-lg-4 col-md-6 mb-4">
                <div class="card environmental-card h-100">
                    <div class="card-body text-center">
                        <div class="env-icon">
                            <i class="bi ${param.icon} text-${param.color}"></i>
                        </div>
                        <h3 class="display-4 text-${param.color}" style="color: ${valueColor} !important;">
                            ${value}${param.unit}
                        </h3>
                        <p class="text-muted mb-0">${param.label}</p>
                        <div class="env-trend mt-2">
                            <small class="text-muted">Live Data</small>
                        </div>
                    </div>
                </div>
            </div>
        `;
    }).join('');

    // Set the HTML content
    container.innerHTML = cardsHTML;

    console.log('Environmental data cards created successfully');
}

function hideAllTabs() {
    const deviceInfo = document.getElementById('deviceInfo');
    const deviceTypeIndicator = document.getElementById('deviceTypeIndicator');
    const tabContent = document.getElementById('deviceTypeTabContent');

    if (deviceInfo) deviceInfo.style.display = 'none';
    if (deviceTypeIndicator) deviceTypeIndicator.style.display = 'none';
    if (tabContent) tabContent.style.display = 'none';

    currentDeviceId = null;

    if (socket && currentDeviceRoom) {
        socket.emit('leave', {device_id: currentDeviceRoom});
        currentDeviceRoom = null;
    }
}

// ========================
// CHART INITIALIZATION (Your existing + enhanced)
// ========================

function initializeCharts() {
    // Ensure Chart.js is loaded
    if (typeof Chart === 'undefined') {
        console.error('Chart.js not loaded');
        setTimeout(initializeCharts, 100); // Retry after a short delay
        return;
    }

    // Register plugins
    try {
        if (window.ChartZoom && typeof Chart.register === 'function') {
            Chart.register(window.ChartZoom);
        }
        if (typeof Chart.register === 'function' && typeof livePulsingPlugin !== 'undefined') {
            Chart.register(livePulsingPlugin);
            console.log('Successfully registered livePulsingPlugin');
        }
    } catch (e) {
        console.warn('Failed to register plugins:', e);
    }

    const commonOptions = {
        responsive: true,
        maintainAspectRatio: false,
        animation: {
            duration: 0 // Disable animations for real-time
        },
        plugins: {
            legend: {
                position: 'top',
                labels: {
                    color: chartTextColor,
                    padding: 20,
                    font: { size: 13 }
                }
            }
        },
        scales: {
            y: {
                beginAtZero: true,
                grid: { 
                    color: chartGridColor,
                    borderDash: [5, 5],
                    drawBorder: false
                },
                ticks: { 
                    color: chartTextColor,
                    precision: 0,
                    font: { size: 12 },
                    maxTicksLimit: 10
                }
            },
            x: {
                grid: { 
                    color: chartGridColor,
                    borderDash: [5, 5],
                    drawBorder: false
                },
                ticks: { color: chartTextColor }
            }
        }
    };

    // Initialize PM Time Chart with rigid center axis and no zoom
    const timeCtx = document.getElementById('timeChart');
    if (timeCtx) {
        charts.timeChart = new Chart(timeCtx.getContext('2d'), {
            type: 'line',
            data: {
                labels: [],
                datasets: [
                    createDatasetConfig('PM1', colorScheme.pm1),
                    createDatasetConfig('PM2.5', colorScheme.pm2_5),
                    createDatasetConfig('PM4', colorScheme.pm4),
                    createDatasetConfig('PM10', colorScheme.pm10),
                    createDatasetConfig('TSP', colorScheme.tsp),
                    // Moving average for PM2.5 (visual aid)
                    Object.assign(createDatasetConfig('PM2.5 MA', 'rgba(120,120,120,0.9)'), {
                        borderDash: [6, 4],
                        borderWidth: 2,
                        pointRadius: 0,
                        pointHoverRadius: 4,
                        backgroundColor: 'rgba(120,120,120,0.06)'
                    })
                ]
            },
            options: {
                ...commonOptions,
                interaction: { mode: 'index', intersect: false },
                plugins: {
                    ...commonOptions.plugins,
                    tooltip: {
                        backgroundColor: 'rgba(0, 0, 0, 0.85)',
                        titleFont: { weight: 'bold' },
                        padding: 10,
                        callbacks: {
                            title: items => {
                                const t = items[0];
                                return t.label ? new Date(t.label).toLocaleString() : '';
                            },
                            label: ctx => {
                                const val = typeof ctx.raw === 'object' && ctx.raw !== null ? ctx.raw.y : ctx.raw;
                                return `${ctx.dataset.label}: ${Number(val).toFixed(1)} μg/m³`;
                            }
                        }
                    },
                    zoom: {
                        pan: {
                            enabled: true,
                            mode: 'x'
                        },
                        zoom: {
                            wheel: {
                                enabled: true
                            },
                            pinch: {
                                enabled: true
                            },
                            mode: 'x'
                        }
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        min: 0,
                        max: 250, // Fixed maximum for PM levels
                        grid: {
                            color: chartGridColor,
                            drawBorder: false,
                            borderDash: [5, 5]
                        },
                        ticks: {
                            color: chartTextColor,
                            padding: 10,
                            callback: function(value) {
                                return value + ' μg/m³';
                            },
                            maxTicksLimit: 8
                        },
                        title: {
                            display: true,
                            text: 'PM Level (μg/m³)',
                            color: chartTextColor,
                            font: {
                                size: 14,
                                weight: 'bold'
                            },
                            padding: { top: 10, bottom: 10 }
                        }
                    },
                    x: {
                        type: 'time',
                        time: {
                            tooltipFormat: 'yyyy-MM-dd HH:mm:ss',
                            displayFormats: {
                                second: 'h:mm:ss a',
                                minute: 'h:mm a',
                                hour: 'h:mm a',
                                day: 'MMM dd'
                            }
                        },
                        grid: {
                            color: chartGridColor,
                            drawBorder: false,
                            borderDash: [5, 5]
                        },
                        ticks: {
                            source: 'auto',
                            color: chartTextColor,
                            padding: 10,
                            maxTicksLimit: 12,
                            autoSkip: true
                        },
                        title: {
                            display: true,
                            text: 'Time (GMT)',
                            color: chartTextColor,
                            font: {
                                size: 14,
                                weight: 'bold'
                            },
                            padding: { top: 10, bottom: 10 }
                        }
                    }
                }
            }
        });
    }
}

function safeChartUpdate(chart, key = 'chart') {
    try {
        if (chart && typeof chart.update === 'function') {
            // For live telemetry charts, use a smooth animation transition instead of 'none'
            if (key === 'timeChart' || key === 'environmentalCombinedChart' || key === 'unifiedChart') {
                chart.update(); // Smoothly animate updates
            } else {
                chart.update('none'); // No animation for heavy analytics
            }
        }
    } catch (err) {
        console.error(`Chart update failed for ${key}:`, err);
        // Attempt to reinitialize the chart if it's broken
        if (charts[key] && document.getElementById(key)) {
            console.log(`Attempting to reinitialize chart: ${key}`);
            setTimeout(() => {
                try {
                    if (key === 'timeChart') initializeCharts();
                    else if (key === 'thresholdChart') initializeCharts();
                    // Add other chart initializations as needed
                } catch (e) {
                    console.error(`Failed to reinitialize chart ${key}:`, e);
                }
            }, 1000);
        }
    }
}
// Helper function to create dataset config (your existing + premium visuals)
function createDatasetConfig(label, borderColor) {
    return {
        label,
        data: [],
        borderColor,
        backgroundColor: function(context) {
            const chart = context.chart;
            const {ctx, chartArea} = chart;
            if (!chartArea) {
                return 'rgba(255, 255, 255, 0.01)'; // Fallback faint default glow
            }
            // Create a gorgeous canvas gradient that fades to fully transparent at the bottom
            const gradient = ctx.createLinearGradient(0, chartArea.top, 0, chartArea.bottom);
            let color = typeof borderColor === 'string' ? borderColor : 'rgba(102, 126, 234, 0.85)';
            let startColor = color.replace('0.85', '0.12').replace('0.8', '0.12');
            let endColor = color.replace('0.85', '0.00').replace('0.8', '0.00');
            gradient.addColorStop(0, startColor);
            gradient.addColorStop(1, endColor);
            return gradient;
        },
        borderWidth: 2,               // sleek, sharp border strokes
        pointRadius: 0,               // hides circles on default view to eliminate visual clutter
        pointHoverRadius: 6,          // highlights specific point beautifully on cursor hover
        pointHoverBackgroundColor: borderColor,
        pointHoverBorderColor: '#fff',
        pointHoverBorderWidth: 2,
        pointStyle: 'circle',
        tension: 0.4,                 // elegant smooth curve calculations
        fill: true                    // enables the fading area under line curves
    };
}

// Custom Chart.js Plugin for real-time live pulsing dot animation at the latest data point
const livePulsingPlugin = {
    id: 'livePulsing',
    afterDatasetsDraw(chart) {
        // Only run on line charts
        if (chart.config.type !== 'line') return;

        const {ctx} = chart;
        chart.data.datasets.forEach((dataset, datasetIndex) => {
            const meta = chart.getDatasetMeta(datasetIndex);
            if (meta.hidden) return;

            const points = meta.data;
            if (points.length === 0) return;

            // Get the last active point with valid coordinate
            let lastPoint = null;
            for (let i = points.length - 1; i >= 0; i--) {
                const pt = points[i];
                if (pt && !isNaN(pt.x) && !isNaN(pt.y)) {
                    lastPoint = pt;
                    break;
                }
            }

            if (lastPoint) {
                const {x, y} = lastPoint;
                const color = dataset.borderColor || 'rgba(102, 126, 234, 0.85)';

                // Pulsing animation math using real-time timestamp
                const time = Date.now() / 1000;
                const pulseRadius = 5 + Math.sin(time * 6) * 3.5;
                const pulseOpacity = 0.35 + Math.sin(time * 6) * 0.15;

                ctx.save();
                
                // Pulsing Halo
                ctx.beginPath();
                ctx.arc(x, y, pulseRadius, 0, 2 * Math.PI);
                ctx.fillStyle = typeof color === 'string' 
                    ? color.replace('0.85', pulseOpacity).replace('0.8', pulseOpacity) 
                    : 'rgba(102, 126, 234, 0.3)';
                ctx.fill();

                // Solid white background ring
                ctx.beginPath();
                ctx.arc(x, y, 4, 0, 2 * Math.PI);
                ctx.fillStyle = '#ffffff';
                ctx.fill();

                // Small center color core
                ctx.beginPath();
                ctx.arc(x, y, 2.5, 0, 2 * Math.PI);
                ctx.fillStyle = color;
                ctx.fill();

                ctx.restore();
            }
        });
    }
};

// Start the pulsing animation loop for the active combined chart
function startLivePulseLoop() {
    const repaint = () => {
        let activeRepaint = false;
        if (charts.environmentalCombinedChart && document.getElementById('environmentalCombinedChart')) {
            charts.environmentalCombinedChart.draw();
            activeRepaint = true;
        }
        if (charts.timeChart && document.getElementById('timeChart')) {
            charts.timeChart.draw();
            activeRepaint = true;
        }
        if (activeRepaint) {
            requestAnimationFrame(repaint);
        } else {
            setTimeout(startLivePulseLoop, 1000); // Retry later if charts not active yet
        }
    };
    requestAnimationFrame(repaint);
}

// ========================
// EXTENDED CHARTS INITIALIZATION (New)
// ========================

function initializeExtendedCharts() {
    const commonOptions = {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
            legend: { position: 'top' },
            zoom: {
                pan: {
                    enabled: true,
                    mode: 'x'
                },
                zoom: {
                    wheel: {
                        enabled: true
                    },
                    pinch: {
                        enabled: true
                    },
                    mode: 'x'
                }
            }
        }
    };

    // Combined Environmental Chart (single graph for each selected device)
    if (document.getElementById('environmentalCombinedChart')) {
        charts.environmentalCombinedChart = new Chart(document.getElementById('environmentalCombinedChart').getContext('2d'), {
            type: 'line',
            data: {
                labels: [],
                datasets: [
                    { ...createDatasetConfig('PM1 (μg/m³)', colorScheme.pm1), yAxisID: 'y-pm', tension: 0.35, hidden: true },
                    { ...createDatasetConfig('PM2.5 (μg/m³)', colorScheme.pm2_5), yAxisID: 'y-pm', tension: 0.35 },
                    { ...createDatasetConfig('PM10 (μg/m³)', colorScheme.pm10), yAxisID: 'y-pm', tension: 0.35 },
                    { ...createDatasetConfig('Temperature (°C)', colorScheme.temperature), yAxisID: 'y-temp', tension: 0.35 },
                    { ...createDatasetConfig('Humidity (%)', colorScheme.humidity), yAxisID: 'y-humid', tension: 0.35 },
                    { ...createDatasetConfig('Pressure (hPa)', colorScheme.pressure), yAxisID: 'y-pressure', tension: 0.35, hidden: true },
                    { ...createDatasetConfig('VOC (ppb)', colorScheme.voc), yAxisID: 'y-air', tension: 0.35, hidden: true },
                    { ...createDatasetConfig('NO2 (ppb)', colorScheme.no2), yAxisID: 'y-air', tension: 0.35, hidden: true },
                    { ...createDatasetConfig('Noise (dB)', colorScheme.noise), yAxisID: 'y-noise', tension: 0.35, hidden: true }
                ]
            },
            options: {
                ...commonOptions,
                interaction: { mode: 'index', intersect: false },
                plugins: {
                    ...commonOptions.plugins,
                    tooltip: {
                        callbacks: {
                            label: ctx => {
                                const val = typeof ctx.raw === 'object' && ctx.raw !== null ? ctx.raw.y : ctx.raw;
                                return `${ctx.dataset.label}: ${Number(val).toFixed(1)}`;
                            }
                        }
                    }
                },
                scales: {
                    x: { 
                        type: 'time',
                        time: {
                            tooltipFormat: 'yyyy-MM-dd HH:mm:ss',
                            displayFormats: {
                                second: 'h:mm:ss a',
                                minute: 'h:mm a',
                                hour: 'h:mm a',
                                day: 'MMM dd'
                            }
                        },
                        ticks: {
                            source: 'auto',
                            maxTicksLimit: 12,
                            autoSkip: true
                        },
                        grid: {
                            color: chartGridColor,
                            borderDash: [5, 5],
                            drawBorder: false
                        }
                    },
                    'y-pm': {
                        type: 'linear',
                        position: 'left',
                        min: 0,
                        title: { display: true, text: 'PM Levels (μg/m³)' },
                        grid: {
                            color: chartGridColor,
                            borderDash: [5, 5],
                            drawBorder: false
                        }
                    },
                    'y-temp': {
                        type: 'linear',
                        position: 'right',
                        min: 0,
                        title: { display: true, text: 'Temp / Humidity' },
                        grid: { drawOnChartArea: false }
                    },
                    'y-humid': {
                        type: 'linear',
                        position: 'left',
                        min: 0,
                        display: false,
                        grid: { drawOnChartArea: false }
                    },
                    'y-pressure': {
                        type: 'linear',
                        position: 'right',
                        min: 0,
                        title: { display: true, text: 'Pressure' },
                        grid: { drawOnChartArea: false }
                    },
                    'y-air': {
                        type: 'linear',
                        position: 'right',
                        min: 0,
                        display: false,
                        grid: { drawOnChartArea: false }
                    },
                    'y-noise': {
                        type: 'linear',
                        position: 'right',
                        min: 0,
                        display: false,
                        grid: { drawOnChartArea: false }
                    }
                }
            }
        });
    }

    // Cloud Environmental Scatter Plot
    if (document.getElementById('cloudEnvironmentalChart')) {
        charts.cloudEnvironmentalChart = new Chart(document.getElementById('cloudEnvironmentalChart').getContext('2d'), {
            type: 'scatter',
            data: {
                datasets: [
                    {
                        label: 'Temperature vs Cloud Cover',
                        data: [],
                        backgroundColor: colorScheme.temperature
                    },
                    {
                        label: 'Humidity vs Cloud Cover',
                        data: [],
                        backgroundColor: colorScheme.humidity
                    }
                ]
            },
            options: {
                ...commonOptions,
                scales: {
                    x: { title: { display: true, text: 'Cloud Cover (%)' } },
                    y: { title: { display: true, text: 'Environmental Values' } }
                }
            }
        });
    }

    // Initialize Unified Combined Chart
    const unifiedCtx = document.getElementById('unifiedChart');
    if (unifiedCtx) {
        charts.unifiedChart = new Chart(unifiedCtx.getContext('2d'), {
            type: 'line',
            data: {
                labels: [],
                datasets: [
                    // PM datasets
                    { ...createDatasetConfig('PM1', colorScheme.pm1), yAxisID: 'y-pm', hidden: true },
                    { ...createDatasetConfig('PM2.5', colorScheme.pm2_5), yAxisID: 'y-pm' },
                    { ...createDatasetConfig('PM4', colorScheme.pm4), yAxisID: 'y-pm', hidden: true },
                    { ...createDatasetConfig('PM10', colorScheme.pm10), yAxisID: 'y-pm' },
                    // Environmental datasets - using premium dataset helper
                    { ...createDatasetConfig('Temperature (°C)', colorScheme.temperature), yAxisID: 'y-temp' },
                    { ...createDatasetConfig('Humidity (%)', colorScheme.humidity), yAxisID: 'y-humid' }
                ]
            },
            options: {
                ...commonOptions,
                responsive: true,
                maintainAspectRatio: false,
                interaction: { mode: 'index', intersect: false },
                plugins: {
                    ...commonOptions.plugins,
                    tooltip: {
                        backgroundColor: 'rgba(0, 0, 0, 0.8)',
                        callbacks: {
                            label: ctx => {
                                const value = ctx.raw.toFixed(2);
                                if (ctx.dataset.label.includes('PM')) return `${ctx.dataset.label}: ${value} μg/m³`;
                                if (ctx.dataset.label.includes('Temperature')) return `${ctx.dataset.label}: ${value} °C`;
                                if (ctx.dataset.label.includes('Humidity')) return `${ctx.dataset.label}: ${value} %`;
                                return `${ctx.dataset.label}: ${value}`;
                            }
                        }
                    },
                    legend: { position: 'top', labels: { usePointStyle: true, padding: 15 } }
                },
                scales: {
                    x: { 
                        type: 'time', 
                        display: true,
                        time: {
                            tooltipFormat: 'yyyy-MM-dd HH:mm:ss',
                            displayFormats: {
                                second: 'h:mm:ss a',
                                minute: 'h:mm a',
                                hour: 'h:mm a',
                                day: 'MMM dd'
                            }
                        },
                        ticks: {
                            source: 'auto',
                            maxTicksLimit: 12,
                            autoSkip: true
                        },
                        grid: {
                            color: chartGridColor,
                            borderDash: [5, 5],
                            drawBorder: false
                        }
                    },
                    'y-pm': {
                        type: 'linear',
                        display: true,
                        position: 'left',
                        title: { display: true, text: 'PM Levels (μg/m³)' },
                        grid: {
                            color: chartGridColor,
                            borderDash: [5, 5],
                            drawBorder: false
                        }
                    },
                    'y-temp': {
                        type: 'linear',
                        display: true,
                        position: 'right',
                        title: { display: true, text: 'Temperature (°C)' },
                        grid: { drawOnChartArea: false }
                    },
                    'y-humid': {
                        type: 'linear',
                        display: false,
                        position: 'right',
                        grid: { drawOnChartArea: false }
                    }
                }
            }
        });
    }

    // Check for missing chart containers and log warnings
    const chartContainers = [
        'timeChart', 'environmentalCombinedChart', 'unifiedChart'
    ];

    chartContainers.forEach(containerId => {
        if (!document.getElementById(containerId)) {
            console.warn(`Chart container ${containerId} not found`);
        }
    });
}

// ========================
// DEEP ANALYTICS CHARTS (New)
// ========================

function initializeDeepAnalyticsCharts() {
    const commonOptions = {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
            legend: { position: 'top' },
            zoom: {
                pan: {
                    enabled: true,
                    mode: 'x'
                },
                zoom: {
                    wheel: {
                        enabled: true
                    },
                    pinch: {
                        enabled: true
                    },
                    mode: 'x'
                }
            }
        }
    };

    // Parameter Trend Chart
    if (document.getElementById('paramTrendChart')) {
        charts.paramTrendChart = new Chart(document.getElementById('paramTrendChart').getContext('2d'), {
            type: 'line',
            data: { 
                labels: [], 
                datasets: [{ 
                    label: 'Parameter Trend', 
                    data: [], 
                    borderColor: 'rgba(75, 192, 192, 0.8)',
                    fill: true,
                    backgroundColor: 'rgba(75, 192, 192, 0.2)'
                }] 
            },
            options: { 
                ...commonOptions, 
                plugins: {
                    ...commonOptions.plugins,
                    tooltip: {
                        callbacks: {
                            label: ctx => {
                                const val = typeof ctx.raw === 'object' && ctx.raw !== null ? ctx.raw.y : ctx.raw;
                                return `${ctx.dataset.label || 'Value'}: ${Number(val).toFixed(1)}`;
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        type: 'time',
                        time: {
                            tooltipFormat: 'yyyy-MM-dd HH:mm:ss',
                            displayFormats: {
                                second: 'h:mm:ss a',
                                minute: 'h:mm a',
                                hour: 'h:mm a',
                                day: 'MMM dd'
                            }
                        },
                        ticks: {
                            source: 'auto',
                            maxTicksLimit: 12,
                            autoSkip: true
                        }
                    }
                } 
            }
        });
    }

    // Daily Statistics Chart
    if (document.getElementById('dailyStatsChart')) {
        charts.dailyStatsChart = new Chart(document.getElementById('dailyStatsChart').getContext('2d'), {
            type: 'bar',
            data: { 
                labels: [], 
                datasets: [
                    { label: 'Min', data: [], backgroundColor: 'rgba(255, 99, 132, 0.5)' },
                    { label: 'Average', data: [], backgroundColor: 'rgba(54, 162, 235, 0.5)' },
                    { label: 'Max', data: [], backgroundColor: 'rgba(75, 192, 192, 0.5)' }
                ] 
            },
            options: commonOptions
        });
    }

    // Correlation Scatter Plot
    if (document.getElementById('correlationScatter')) {
        charts.correlationScatter = new Chart(document.getElementById('correlationScatter').getContext('2d'), {
            type: 'scatter',
            data: { datasets: [] },
            options: {
                ...commonOptions,
                scales: {
                    x: { title: { display: true, text: 'X Parameter' } },
                    y: { title: { display: true, text: 'Y Parameter' } }
                }
            }
        });
    }

    // Histogram Chart
    if (document.getElementById('histogramChart')) {
        charts.histogramChart = new Chart(document.getElementById('histogramChart').getContext('2d'), {
            type: 'bar',
            data: { 
                labels: [], 
                datasets: [{ 
                    label: 'Frequency', 
                    data: [], 
                    backgroundColor: 'rgba(75, 192, 192, 0.6)'
                }] 
            },
            options: commonOptions
        });
    }

    // Data Completeness Chart
    if (document.getElementById('dataCompletenessChart')) {
        charts.dataCompletenessChart = new Chart(document.getElementById('dataCompletenessChart').getContext('2d'), {
            type: 'line',
            data: { 
                labels: [], 
                datasets: [{ 
                    label: '% Data Available', 
                    data: [], 
                    borderColor: 'rgba(75, 192, 192, 0.8)',
                    backgroundColor: 'rgba(75, 192, 192, 0.2)',
                    fill: true
                }] 
            },
            options: { 
                ...commonOptions, 
                scales: { 
                    x: { type: 'time' },
                    y: { max: 100, ticks: { callback: value => `${value}%` } }
                } 
            }
        });
    }

    // Live MQTT Graph
    if (document.getElementById('mqttLiveChart')) {
        charts.mqttLiveChart = new Chart(document.getElementById('mqttLiveChart').getContext('2d'), {
            type: 'line',
            data: {
                labels: [],
                datasets: [
                    {
                        label: 'Temperature (°C)',
                        data: [],
                        borderColor: colorScheme.temperature,
                        backgroundColor: colorScheme.temperature.replace('0.8', '0.15'),
                        tension: 0.3,
                        spanGaps: true
                    },
                    {
                        label: 'Humidity (%)',
                        data: [],
                        borderColor: colorScheme.humidity,
                        backgroundColor: colorScheme.humidity.replace('0.8', '0.15'),
                        tension: 0.3,
                        spanGaps: true
                    },
                    {
                        label: 'PM2.5',
                        data: [],
                        borderColor: colorScheme.pm2_5,
                        backgroundColor: colorScheme.pm2_5.replace('0.8', '0.15'),
                        tension: 0.3,
                        spanGaps: true
                    }
                ]
            },
            options: {
                ...commonOptions,
                interaction: { mode: 'index', intersect: false },
                scales: {
                    x: { type: 'time' },
                    y: {
                        beginAtZero: false,
                        grid: { color: chartGridColor },
                        ticks: { color: chartTextColor }
                    }
                }
            }
        });
    }

    // Setup parameter dropdowns
    setupParameterDropdowns();
}

function updateMqttLiveChart(mqttData) {
    if (!mqttData) return;

    if (!charts.mqttLiveChart) {
        const canvas = document.getElementById('mqttLiveChart');
        if (!canvas) return;

        charts.mqttLiveChart = new Chart(canvas.getContext('2d'), {
            type: 'line',
            data: {
                labels: [],
                datasets: [
                    {
                        label: 'Temperature (°C)',
                        data: [],
                        borderColor: colorScheme.temperature,
                        backgroundColor: colorScheme.temperature.replace('0.8', '0.15'),
                        tension: 0.3,
                        spanGaps: true
                    },
                    {
                        label: 'Humidity (%)',
                        data: [],
                        borderColor: colorScheme.humidity,
                        backgroundColor: colorScheme.humidity.replace('0.8', '0.15'),
                        tension: 0.3,
                        spanGaps: true
                    },
                    {
                        label: 'PM2.5',
                        data: [],
                        borderColor: colorScheme.pm2_5,
                        backgroundColor: colorScheme.pm2_5.replace('0.8', '0.15'),
                        tension: 0.3,
                        spanGaps: true
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: { mode: 'index', intersect: false },
                plugins: {
                    legend: { position: 'top' }
                },
                scales: {
                    x: { type: 'time' },
                    y: {
                        beginAtZero: false,
                        grid: { color: chartGridColor },
                        ticks: { color: chartTextColor }
                    }
                }
            }
        });
    }

    const rawData = mqttData.raw_data || {};
    const timestamp = new Date(rawData.ts || rawData.t || rawData.timestamp || Date.now());
    const label = timestamp;

    const getNumber = (...values) => {
        for (const value of values) {
            const parsed = Number(value);
            if (Number.isFinite(parsed)) {
                return parsed;
            }
        }
        return null;
    };

    const temperature = getNumber(rawData.tsi_temp, mqttData.extended?.temperature_c);
    const humidity = getNumber(rawData.tsi_rh, mqttData.extended?.humidity_percent);
    const pm25 = getNumber(rawData.tsi_pm25, mqttData.sensor?.pm2_5, rawData.pm2_5, rawData.PM2_5);
    const pm1 = getNumber(rawData.tsi_pm1, mqttData.sensor?.pm1, rawData.pm1, rawData.PM1);
    const pm4 = getNumber(rawData.tsi_pm4, mqttData.sensor?.pm4, rawData.pm4, rawData.PM4);
    const pm10 = getNumber(rawData.tsi_pm10, mqttData.sensor?.pm10, rawData.pm10, rawData.PM10);

    mqttLiveSeries.labels.push(label);
    mqttLiveSeries.temperature.push(temperature);
    mqttLiveSeries.humidity.push(humidity);
    mqttLiveSeries.pm1.push(pm1);
    mqttLiveSeries.pm25.push(pm25);
    mqttLiveSeries.pm4.push(pm4);
    mqttLiveSeries.pm10.push(pm10);

    const maxPoints = 30;
    if (mqttLiveSeries.labels.length > maxPoints) {
        mqttLiveSeries.labels.shift();
        mqttLiveSeries.temperature.shift();
        mqttLiveSeries.humidity.shift();
        mqttLiveSeries.pm1.shift();
        mqttLiveSeries.pm25.shift();
        mqttLiveSeries.pm4.shift();
        mqttLiveSeries.pm10.shift();
    }

    charts.mqttLiveChart.data.labels = mqttLiveSeries.labels;
    charts.mqttLiveChart.data.datasets[0].data = mqttLiveSeries.temperature;
    charts.mqttLiveChart.data.datasets[1].data = mqttLiveSeries.humidity;
    charts.mqttLiveChart.data.datasets[2].data = mqttLiveSeries.pm25;
    safeChartUpdate(charts.mqttLiveChart, 'mqttLiveChart');

    if (!charts.mqttValueChart) {
        const valueCanvas = document.getElementById('mqttValueChart');
        if (valueCanvas) {
            charts.mqttValueChart = new Chart(valueCanvas.getContext('2d'), {
                type: 'bar',
                data: {
                    labels: ['Temperature', 'Humidity', 'PM1.0', 'PM2.5', 'PM4.0', 'PM10'],
                    datasets: [{
                        label: 'Latest MQTT Values',
                        data: [null, null, null, null, null, null],
                        backgroundColor: [
                            colorScheme.temperature,
                            colorScheme.humidity,
                            colorScheme.pm1,
                            colorScheme.pm2_5,
                            colorScheme.pm4,
                            colorScheme.pm10
                        ],
                        borderWidth: 1
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { display: false }
                    },
                    scales: {
                        y: {
                            beginAtZero: true,
                            grid: { color: chartGridColor },
                            ticks: { color: chartTextColor }
                        },
                        x: {
                            grid: { color: chartGridColor },
                            ticks: { color: chartTextColor }
                        }
                    }
                }
            });
        }
    }

    if (charts.mqttValueChart) {
        charts.mqttValueChart.data.datasets[0].data = [
            temperature,
            humidity,
            pm1,
            pm25,
            pm4,
            pm10
        ];
        safeChartUpdate(charts.mqttValueChart, 'mqttValueChart');
    }
}

function setupParameterDropdowns() {
    const paramSelects = ['paramSelect', 'corrParamX', 'corrParamY', 'histParamSelect'];
    const parameters = [
        { key: 'pm1', label: 'PM1' },
        { key: 'pm2_5', label: 'PM2.5' },
        { key: 'pm4', label: 'PM4' },
        { key: 'pm10', label: 'PM10' },
        { key: 'tsp', label: 'TSP' },
        { key: 'temperature_c', label: 'Temperature (°C)' },
        { key: 'humidity_percent', label: 'Humidity (%)' },
        { key: 'pressure_hpa', label: 'Pressure (hPa)' },
        { key: 'voc_ppb', label: 'VOC (ppb)' },
        { key: 'no2_ppb', label: 'NO₂ (ppb)' },
        { key: 'gps_speed_kmh', label: 'Speed (km/h)' },
        { key: 'cloud_cover_percent', label: 'Cloud Cover (%)' }
    ];

    paramSelects.forEach(selectId => {
        const select = document.getElementById(selectId);
        if (select) {
            parameters.forEach(param => {
                const option = document.createElement('option');
                option.value = param.key;
                option.textContent = param.label;
                select.appendChild(option);
            });

            // Add event listeners for dynamic updates
            select.addEventListener('change', function() {
                if (selectId === 'paramSelect') {
                    updateParameterTrendChart(this.value);
                } else if (selectId === 'histParamSelect') {
                    updateHistogramChart(this.value);
                } else if (selectId.startsWith('corr')) {
                    updateCorrelationChart();
                }
            });
        }
    });
}

// ========================
// UPDATE CHARTS THEME (Enhanced from your existing)
// ========================

function updateChartsTheme() {
    chartGridColor = getComputedStyle(document.documentElement).getPropertyValue('--chart-grid').trim() || 'rgba(0, 0, 0, 0.1)';
    chartTextColor = getComputedStyle(document.documentElement).getPropertyValue('--text-color').trim() || '#212529';

    // Update all charts
    Object.values(charts).forEach(chart => {
        if (!chart || !chart.options) return;
        
        // Update legend colors
        if (chart.options.plugins && chart.options.plugins.legend) {
            chart.options.plugins.legend.labels.color = chartTextColor;
        }
        
        // Update scale colors
        if (chart.options.scales) {
            Object.values(chart.options.scales).forEach(scale => {
                if (scale.grid) scale.grid.color = chartGridColor;
                if (scale.ticks) scale.ticks.color = chartTextColor;
                if (scale.title) scale.title.color = chartTextColor;
            });
        }
        
        safeChartUpdate(chart, 'theme');
    });
}

// Remove threshold chart references from safeChartUpdate
function safeChartUpdate(chart, key = 'chart') {
    try {
        if (chart && typeof chart.update === 'function') {
            // For live telemetry charts, use a smooth animation transition instead of 'none'
            if (key === 'timeChart' || key === 'environmentalCombinedChart' || key === 'unifiedChart') {
                chart.update(); // Smoothly animate updates
            } else {
                chart.update('none'); // No animation for heavy analytics
            }
        }
    } catch (err) {
        console.error(`Chart update failed for ${key}:`, err);
    }
}

// ========================
// DATA FETCHING (Enhanced from your existing)
// ========================

function fetchData(hours = 24, showSpinner = true) {
    if (!currentDeviceId) {
        createAlert('Please select a device first', 'warning');
        return;
    }

    const loadingIndicator = showSpinner ? showLoading() : null;
    
    apiFetch(`/api/data?hours=${encodeURIComponent(hours)}&avg_window=${encodeURIComponent(currentAvgWindow)}&deviceid=${encodeURIComponent(currentDeviceId)}`)
        .then(response => {
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            return response.json();
        })
        .then(data => {
            safeProcessIncomingData(data); // Use safe processing
        })
        .catch(error => {
            console.error('Error fetching data:', error);
            if (showSpinner) {
                createAlert('Failed to fetch data. Please try again.', 'danger');
            }
        })
        .finally(() => {
            if (loadingIndicator && loadingIndicator.parentNode === document.body) {
                document.body.removeChild(loadingIndicator);
            }
        });
}

function startRealtimePolling() {
    if (pollingIntervalId || !currentDeviceId) return;
    pollingIntervalId = setInterval(() => {
        if (currentDeviceId) fetchData(0.25);
    }, 5000);
}

function stopRealtimePolling() {
    if (pollingIntervalId) {
        clearInterval(pollingIntervalId);
        pollingIntervalId = null;
    }
}

// ========================
// DASHBOARD UPDATE (Enhanced from your existing)
// ========================

function updateDashboard(data) {
    // Update connection status
    const connectionStatus = document.getElementById('connectionStatus') || document.getElementById('deviceStatus');
    const navConnectionStatus = document.getElementById('navConnectionStatus');
    const connectionDot = document.getElementById('connectionDot');
    
    if (connectionStatus) {
        connectionStatus.textContent = 'ONLINE';
        connectionStatus.className = 'badge bg-success';
    }
    
    if (navConnectionStatus) {
        navConnectionStatus.textContent = 'Online';
    }
    
    if (connectionDot) {
        connectionDot.className = 'status-dot online';
    }
    
    // Update last update time
    if (data.sensor && data.sensor.timestamp) {
        const lastUpdate = document.getElementById('lastUpdate');
        const readingsTime = document.getElementById('readingsTime');
        const lastUpdateTime = document.getElementById('lastUpdateTime');
        
        const timeStr = new Date(data.sensor.timestamp).toLocaleTimeString();
        
        if (lastUpdate) lastUpdate.textContent = new Date(data.sensor.timestamp).toLocaleString();
        if (readingsTime) readingsTime.textContent = timeStr;
        if (lastUpdateTime) lastUpdateTime.textContent = timeStr;
    }
    
    // Update PM readings
    updatePMReadings(data.sensor);
    
    // Update relay status if applicable
    if (data.status && data.status.relay_state !== 'N/A') {
        const relayState = document.getElementById('relayState');
        if (relayState) {
            relayState.textContent = data.status.relay_state;
            relayState.className = `badge ${data.status.relay_state === 'ON' ? 'bg-success' : 'bg-secondary'}`;
        }
    }
}

function updatePMReadings(sensor) {
    const pmReadings = document.getElementById('pmReadings');
    
    if (!pmReadings || !sensor) {
        if (pmReadings) pmReadings.innerHTML = '<p class="text-muted">No data available</p>';
        return;
    }
    
    const pmTypes = ['pm1', 'pm2_5', 'pm4', 'pm10', 'tsp'];
    const pmLabels = ['PM1', 'PM2.5', 'PM4', 'PM10', 'TSP'];
    
    pmReadings.innerHTML = pmTypes.map((type, index) => {
        const value = sensor[type] || 0;
        const percentage = Math.min(value / 200 * 100, 100);
        
        return `
            <div class="pm-reading-item">
                <div class="d-flex justify-content-between align-items-center w-100">
                    <span class="fw-bold">${pmLabels[index]}</span>
                    <span class="badge ${getAQIColor(value)} fs-6">${value.toFixed(1)} μg/m³</span>
                </div>
                <div class="progress mt-2" style="height: 6px;">
                    <div class="progress-bar ${getProgressBarColor(value)}" 
                         style="width: ${percentage}%"></div>
                </div>
            </div>
        `;
    }).join('');
    
    // Update legend statistics
    pmTypes.forEach((type, index) => {
        const statElement = document.getElementById(`${type.replace('_', '')}-stat`);
        if (statElement) {
            statElement.textContent = `${(sensor[type] || 0).toFixed(1)} μg/m³`;
        }
    });
}

function getProgressBarColor(value) {
    if (value < 50) return 'bg-success';
    if (value < 100) return 'bg-warning';
    if (value < 200) return 'bg-danger';
    return 'bg-dark';
}

function getAQIColor(value) {
    if (value < 50) return 'bg-success';
    if (value < 100) return 'bg-warning';
    if (value < 200) return 'bg-danger';
    return 'bg-dark';
}

// ========================
// CHART UPDATES (Enhanced from your existing)
// ========================

// ========================
// CHART UPDATE FUNCTIONS (Complete Fixed Version)
// ========================
function updateCharts(data) {
    if (!data || !data.history) {
        console.warn('No history data available for charts');
        return;
    }

    // --- Chronological Sorting & Timeline Merging & Alignment ---
    const pmTimestamps = data.history.timestamps || [];
    const extHistory = data.history.extended || {};
    const extTimestamps = extHistory.timestamps || [];
    
    // Helper to safely parse and normalize timestamp representations
    const parseTime = (t) => {
        if (!t) return null;
        const s = typeof t === 'string' ? t.replace(' ', 'T') : t;
        const d = new Date(s);
        return isNaN(d.getTime()) ? null : d;
    };

    // 1. Sort standard PM data chronologically
    const pmPoints = [];
    for (let i = 0; i < pmTimestamps.length; i++) {
        const d = parseTime(pmTimestamps[i]);
        if (d) {
            pmPoints.push({
                date: d,
                pm1: data.history.pm1 ? data.history.pm1[i] : null,
                pm2_5: data.history.pm2_5 ? data.history.pm2_5[i] : null,
                pm4: data.history.pm4 ? data.history.pm4[i] : null,
                pm10: data.history.pm10 ? data.history.pm10[i] : null,
                tsp: data.history.tsp ? data.history.tsp[i] : null
            });
        }
    }
    pmPoints.sort((a, b) => a.date.getTime() - b.date.getTime());
    
    const sortedPmTimestamps = pmPoints.map(p => p.date);
    const sortedPm1 = pmPoints.map(p => p.pm1);
    const sortedPm2_5 = pmPoints.map(p => p.pm2_5);
    const sortedPm4 = pmPoints.map(p => p.pm4);
    const sortedPm10 = pmPoints.map(p => p.pm10);
    const sortedTsp = pmPoints.map(p => p.tsp);

    // 2. Sort extended environmental data chronologically
    const extPoints = [];
    for (let i = 0; i < extTimestamps.length; i++) {
        const d = parseTime(extTimestamps[i]);
        if (d) {
            extPoints.push({
                date: d,
                temperature_c: extHistory.temperature_c ? extHistory.temperature_c[i] : null,
                humidity_percent: extHistory.humidity_percent ? extHistory.humidity_percent[i] : null
            });
        }
    }
    extPoints.sort((a, b) => a.date.getTime() - b.date.getTime());
    
    const sortedExtTimestamps = extPoints.map(p => p.date);
    const sortedTemp = extPoints.map(p => p.temperature_c);
    const sortedHumid = extPoints.map(p => p.humidity_percent);

    // 3. Merge standard and extended timelines for unified charts
    const combinedUniqueTimes = new Set();
    sortedPmTimestamps.forEach(d => combinedUniqueTimes.add(d.getTime()));
    sortedExtTimestamps.forEach(d => combinedUniqueTimes.add(d.getTime()));
    
    const sortedCombinedTimes = Array.from(combinedUniqueTimes).sort((a, b) => a - b);
    const sortedCombinedTimestamps = sortedCombinedTimes.map(t => new Date(t));

    // Create maps for alignment
    const pmMap = { pm1: new Map(), pm2_5: new Map(), pm4: new Map(), pm10: new Map() };
    for (let i = 0; i < sortedPmTimestamps.length; i++) {
        const timeMs = sortedPmTimestamps[i].getTime();
        if (sortedPm1[i] !== null && sortedPm1[i] !== undefined) pmMap.pm1.set(timeMs, sortedPm1[i]);
        if (sortedPm2_5[i] !== null && sortedPm2_5[i] !== undefined) pmMap.pm2_5.set(timeMs, sortedPm2_5[i]);
        if (sortedPm4[i] !== null && sortedPm4[i] !== undefined) pmMap.pm4.set(timeMs, sortedPm4[i]);
        if (sortedPm10[i] !== null && sortedPm10[i] !== undefined) pmMap.pm10.set(timeMs, sortedPm10[i]);
    }
    
    const extMap = { temp: new Map(), humid: new Map() };
    for (let i = 0; i < sortedExtTimestamps.length; i++) {
        const timeMs = sortedExtTimestamps[i].getTime();
        if (sortedTemp[i] !== null && sortedTemp[i] !== undefined) extMap.temp.set(timeMs, sortedTemp[i]);
        if (sortedHumid[i] !== null && sortedHumid[i] !== undefined) extMap.humid.set(timeMs, sortedHumid[i]);
    }

    const alignedPm1 = sortedCombinedTimes.map(t => pmMap.pm1.has(t) ? Number(pmMap.pm1.get(t)) : null);
    const alignedPm2_5 = sortedCombinedTimes.map(t => pmMap.pm2_5.has(t) ? Number(pmMap.pm2_5.get(t)) : null);
    const alignedPm4 = sortedCombinedTimes.map(t => pmMap.pm4.has(t) ? Number(pmMap.pm4.get(t)) : null);
    const alignedPm10 = sortedCombinedTimes.map(t => pmMap.pm10.has(t) ? Number(pmMap.pm10.get(t)) : null);
    
    const alignedTemp = sortedCombinedTimes.map(t => extMap.temp.has(t) ? Number(extMap.temp.get(t)) : null);
    const alignedHumid = sortedCombinedTimes.map(t => extMap.humid.has(t) ? Number(extMap.humid.get(t)) : null);

    // Update PM Time Chart
    if (charts.timeChart) {
        charts.timeChart.data.labels = []; // Clear to prevent category timeline overlap
        
        let targetDate = new Date();
        if (sortedPmTimestamps && sortedPmTimestamps.length > 0) {
            targetDate = new Date(sortedPmTimestamps[sortedPmTimestamps.length - 1]);
        }
        setChartDayBounds(charts.timeChart, targetDate);

        if (sortedPmTimestamps.length > 0) {
            charts.timeChart.data.datasets[0].data = sortedPmTimestamps.map((t, idx) => ({ x: t, y: sortedPm1[idx] }));
            charts.timeChart.data.datasets[1].data = sortedPmTimestamps.map((t, idx) => ({ x: t, y: sortedPm2_5[idx] }));
            charts.timeChart.data.datasets[2].data = sortedPmTimestamps.map((t, idx) => ({ x: t, y: sortedPm4[idx] }));
            charts.timeChart.data.datasets[3].data = sortedPmTimestamps.map((t, idx) => ({ x: t, y: sortedPm10[idx] }));
            charts.timeChart.data.datasets[4].data = sortedPmTimestamps.map((t, idx) => ({ x: t, y: sortedTsp[idx] }));

            // Compute moving average
            try {
                const pm = (sortedPm2_5 || []).map(Number);
                const n = pm.length;
                const windowSize = n > 24 ? 12 : (n > 8 ? 6 : 3);
                const ma = new Array(n).fill(null);
                for (let i = 0; i < n; i++) {
                    const start = Math.max(0, i - windowSize + 1);
                    const slice = pm.slice(start, i + 1).filter(v => !isNaN(v));
                    if (slice.length) ma[i] = slice.reduce((a, b) => a + b, 0) / slice.length;
                }
                if (charts.timeChart.data.datasets[5]) {
                    charts.timeChart.data.datasets[5].data = sortedPmTimestamps.map((t, idx) => ({ x: t, y: ma[idx] }));
                }
            } catch (e) {
                console.warn('Failed to compute moving average:', e);
            }
        } else {
            charts.timeChart.data.datasets.forEach(ds => ds.data = []);
        }

        // Handle suggestedMax and rigid axes logic safely
        try {
            const allVals = [].concat(
                sortedPm1 || [],
                sortedPm2_5 || [],
                sortedPm4 || [],
                sortedPm10 || [],
                sortedTsp || []
            ).map(Number).filter(v => !isNaN(v) && v != null);

            const maxVal = allVals.length ? Math.max(...allVals) : 50;

            if (rigidAxesEnabled) {
                const rigidMax = Math.max(50, Math.ceil(maxVal * 1.1 / 50) * 50);
                rigidMaxByChart['timeChart'] = rigidMax;
                if (!charts.timeChart.options.scales) charts.timeChart.options.scales = {};
                if (!charts.timeChart.options.scales.y) charts.timeChart.options.scales.y = {};
                charts.timeChart.options.scales.y.max = rigidMax;
            } else {
                const buffer = Math.max(10, Math.round(maxVal * 0.25));
                const suggestedMax = Math.max(50, Math.ceil((maxVal + buffer) / 10) * 10);
                if (!charts.timeChart.options.scales) charts.timeChart.options.scales = {};
                if (!charts.timeChart.options.scales.y) charts.timeChart.options.scales.y = {};
                charts.timeChart.options.scales.y.suggestedMax = suggestedMax;
                delete charts.timeChart.options.scales.y.max;
            }
        } catch (e) {
            console.warn('Failed to compute suggestedMax/rigidMax for chart:', e);
        }

        safeChartUpdate(charts.timeChart, 'timeChart');
    }

    // Update Unified Chart
    if (charts.unifiedChart) {
        const finalTempData = alignedTemp.some(v => v !== null) ? alignedTemp : 
            (data.extended?.temperature_c ? Array(sortedCombinedTimestamps.length).fill(Number(data.extended.temperature_c)) : []);
        const finalHumidityData = alignedHumid.some(v => v !== null) ? alignedHumid : 
            (data.extended?.humidity_percent ? Array(sortedCombinedTimestamps.length).fill(Number(data.extended.humidity_percent)) : []);

        charts.unifiedChart.data.labels = []; // Clear labels
        
        let targetDate = new Date();
        if (sortedCombinedTimestamps && sortedCombinedTimestamps.length > 0) {
            targetDate = new Date(sortedCombinedTimestamps[sortedCombinedTimestamps.length - 1]);
        }
        setChartDayBounds(charts.unifiedChart, targetDate);

        if (sortedCombinedTimestamps.length > 0) {
            charts.unifiedChart.data.datasets[0].data = sortedCombinedTimestamps.map((t, idx) => ({ x: t, y: alignedPm1[idx] }));
            charts.unifiedChart.data.datasets[1].data = sortedCombinedTimestamps.map((t, idx) => ({ x: t, y: alignedPm2_5[idx] }));
            charts.unifiedChart.data.datasets[2].data = sortedCombinedTimestamps.map((t, idx) => ({ x: t, y: alignedPm4[idx] }));
            charts.unifiedChart.data.datasets[3].data = sortedCombinedTimestamps.map((t, idx) => ({ x: t, y: alignedPm10[idx] }));
            charts.unifiedChart.data.datasets[4].data = sortedCombinedTimestamps.map((t, idx) => ({ x: t, y: finalTempData[idx] }));
            charts.unifiedChart.data.datasets[5].data = sortedCombinedTimestamps.map((t, idx) => ({ x: t, y: finalHumidityData[idx] }));
        } else {
            charts.unifiedChart.data.datasets.forEach(ds => ds.data = []);
        }

        safeChartUpdate(charts.unifiedChart, 'unifiedChart');
        updateUnifiedChartStatistics(data);
    }

    // Update threshold comparison chart
    if (charts.thresholdChart && data.sensor) {
        const currentValues = [
            data.sensor.pm1 || 0,
            data.sensor.pm2_5 || 0,
            data.sensor.pm4 || 0,
            data.sensor.pm10 || 0,
            data.sensor.tsp || 0
        ];
        
        const thresholdValues = data.status && data.status.thresholds ? [
            data.status.thresholds.pm1 || 50,
            data.status.thresholds['pm2.5'] || 75,
            data.status.thresholds.pm4 || 100,
            data.status.thresholds.pm10 || 150,
            data.status.thresholds.tsp || 200
        ] : [50, 75, 100, 150, 200];
        
        charts.thresholdChart.data.datasets[0].data = currentValues;
        charts.thresholdChart.data.datasets[1].data = thresholdValues;
        
        safeChartUpdate(charts.thresholdChart, 'thresholdChart');
    }
}

function debugDataFlow(data, stage) {
    console.group(`Data Flow: ${stage}`);
    console.log('Raw data structure:', data);
    if (data && data.sensor) console.log('Sensor data:', data.sensor);
    if (data && data.extended) console.log('Extended data:', data.extended);
    if (data && data.history) console.log('History data keys:', Object.keys(data.history));
    console.groupEnd();
}

function updateDeepAnalyticsCharts(data) {
    if (!data.history || !data.history.timestamps) return;
    
    const timestamps = data.history.timestamps.map(t => parseLocalTime(t));
    
    // Parameter Trend Chart (default to PM2.5)
    if (charts.paramTrendChart) {
        const selectedParam = document.getElementById('paramSelect')?.value || 'pm2_5';
        const paramData = data.history[selectedParam] || data.history.pm2_5 || [];
        
        charts.paramTrendChart.data.labels = [];
        
        let targetDate = new Date();
        if (timestamps && timestamps.length > 0) {
            targetDate = new Date(timestamps[timestamps.length - 1]);
        }
        setChartDayBounds(charts.paramTrendChart, targetDate);

        if (timestamps.length > 0) {
            charts.paramTrendChart.data.datasets[0].data = timestamps.map((t, idx) => ({ x: t, y: paramData[idx] }));
        } else {
            charts.paramTrendChart.data.datasets[0].data = [];
        }
        charts.paramTrendChart.data.datasets[0].label = getParameterLabel(selectedParam);
        
        safeChartUpdate(charts.paramTrendChart, 'paramTrendChart');
    }
    
    // Daily Statistics Chart (mock data for now)
    if (charts.dailyStatsChart && data.history.pm2_5) {
        const pm25Data = data.history.pm2_5.filter(v => v != null);
        if (pm25Data.length > 0) {
            const min = Math.min(...pm25Data);
            const max = Math.max(...pm25Data);
            const avg = pm25Data.reduce((a, b) => a + b, 0) / pm25Data.length;
            
            charts.dailyStatsChart.data.labels = ['Today'];
            charts.dailyStatsChart.data.datasets[0].data = [min];
            charts.dailyStatsChart.data.datasets[1].data = [avg];
            charts.dailyStatsChart.data.datasets[2].data = [max];
            
            safeChartUpdate(charts.dailyStatsChart, 'dailyStatsChart');
        }
    }
    
    // Data Completeness Chart
    if (charts.dataCompletenessChart) {
        const totalPoints = timestamps.length;
        const validPoints = data.history.pm2_5.filter(v => v != null && !isNaN(v)).length;
        const completeness = totalPoints > 0 ? (validPoints / totalPoints) * 100 : 0;
        
        charts.dataCompletenessChart.data.labels = timestamps;
        charts.dataCompletenessChart.data.datasets[0].data = Array(totalPoints).fill(completeness);
        
        safeChartUpdate(charts.dataCompletenessChart, 'dataCompletenessChart');
    }
}

function startRealtimePolling() {
    // Prevent multiple polling instances
    if (pollingIntervalId || !currentDeviceId) return;

    console.log('Starting real-time polling (fallback mode)');
    pollingIntervalId = setInterval(() => {
        if (currentDeviceId && (!socket || !socket.connected)) {
            console.log('Polling for new data (WebSocket unavailable)...');
            fetchData(0.25); // Get last 15 minutes of data
        }
    }, 30000); // Poll every 30 seconds (reduced from 5 seconds)
}

function stopRealtimePolling() {
    if (pollingIntervalId) {
        console.log('Stopping real-time polling');
        clearInterval(pollingIntervalId);
        pollingIntervalId = null;
    }
}

function startAutoUpdateTimer() {
    if (autoUpdateIntervalId) {
        clearInterval(autoUpdateIntervalId);
    }
    console.log('Starting 10-second auto-update timer for device:', currentDeviceId);
    autoUpdateIntervalId = setInterval(() => {
        if (currentDeviceId) {
            console.log('Auto-updating dashboard (10s poll)...');
            fetchData(24, false); // Fetch last 24h data silently
        }
    }, 10000);
}

function stopAutoUpdateTimer() {
    if (autoUpdateIntervalId) {
        console.log('Stopping auto-update timer');
        clearInterval(autoUpdateIntervalId);
        autoUpdateIntervalId = null;
    }
}

// Add missing parameter trend chart update function
function updateParameterTrendChart(parameter) {
    if (!currentDeviceId) return;
    
    // Fetch current data and update chart
    apiFetch(`/api/data?hours=24&deviceid=${currentDeviceId}`)
        .then(response => response.json())
        .then(data => {
            if (charts.paramTrendChart && data.history) {
                const timestamps = data.history.timestamps.map(t => parseLocalTime(t));
                const paramData = data.history[parameter] || [];
                
                charts.paramTrendChart.data.labels = [];
                
                let targetDate = new Date();
                if (timestamps && timestamps.length > 0) {
                    targetDate = new Date(timestamps[timestamps.length - 1]);
                }
                setChartDayBounds(charts.paramTrendChart, targetDate);

                if (timestamps.length > 0) {
                    charts.paramTrendChart.data.datasets[0].data = timestamps.map((t, idx) => ({ x: t, y: paramData[idx] }));
                } else {
                    charts.paramTrendChart.data.datasets[0].data = [];
                }
                charts.paramTrendChart.data.datasets[0].label = getParameterLabel(parameter);
                
                safeChartUpdate(charts.paramTrendChart, 'paramTrendChart');
            }
        })
        .catch(error => console.error('Error updating parameter trend:', error));
}

// Add missing correlation chart update function
function updateCorrelationChart() {
    const paramX = document.getElementById('corrParamX')?.value;
    const paramY = document.getElementById('corrParamY')?.value;
    
    if (!paramX || !paramY || !currentDeviceId) return;
    
    apiFetch(`/api/data?hours=24&deviceid=${currentDeviceId}`)
        .then(response => response.json())
        .then(data => {
            if (charts.correlationScatter && data.history) {
                const xData = data.history[paramX] || [];
                const yData = data.history[paramY] || [];
                
                const scatterData = xData.map((x, i) => ({
                    x: x || 0,
                    y: yData[i] || 0
                }));
                
                charts.correlationScatter.data.datasets = [{
                    label: `${getParameterLabel(paramX)} vs ${getParameterLabel(paramY)}`,
                    data: scatterData,
                    backgroundColor: 'rgba(75, 192, 192, 0.6)'
                }];
                
                charts.correlationScatter.options.scales.x.title.text = getParameterLabel(paramX);
                charts.correlationScatter.options.scales.y.title.text = getParameterLabel(paramY);
                
                safeChartUpdate(charts.correlationScatter, 'correlationScatter');
            }
        })
        .catch(error => console.error('Error updating correlation chart:', error));
}

// Add missing histogram chart update function
function updateHistogramChart(parameter) {
    if (!currentDeviceId) return;
    
    apiFetch(`/api/data?hours=24&deviceid=${currentDeviceId}`)
        .then(response => response.json())
        .then(data => {
            if (charts.histogramChart && data.history) {
                const paramData = data.history[parameter] || [];
                const validData = paramData.filter(v => v != null && !isNaN(v));
                
                if (validData.length > 0) {
                    // Create histogram bins
                    const min = Math.min(...validData);
                    const max = Math.max(...validData);
                    const binCount = 10;
                    const binSize = (max - min) / binCount;
                    
                    const bins = Array(binCount).fill(0);
                    const binLabels = [];
                    
                    for (let i = 0; i < binCount; i++) {
                        const binStart = min + i * binSize;
                        const binEnd = min + (i + 1) * binSize;
                        binLabels.push(`${binStart.toFixed(1)}-${binEnd.toFixed(1)}`);
                    }
                    
                    validData.forEach(value => {
                        const binIndex = Math.min(Math.floor((value - min) / binSize), binCount - 1);
                        bins[binIndex]++;
                    });
                    
                    charts.histogramChart.data.labels = binLabels;
                    charts.histogramChart.data.datasets[0].data = bins;
                    charts.histogramChart.data.datasets.label = `${getParameterLabel(parameter)} Distribution`;
                    
                    safeChartUpdate(charts.histogramChart, 'histogramChart');
                }
            }
        })
        .catch(error => console.error('Error updating histogram:', error));
}

function calculateDataCompleteness(history, timestamps) {
    // Simple completeness calculation
    const windowSize = 10;
    const allParams = ['pm1', 'pm2_5', 'pm4', 'pm10', 'tsp'];
    
    return timestamps.map((timestamp, i) => {
        const start = Math.max(0, i - windowSize);
        const end = Math.min(timestamps.length, i + windowSize);
        const window = end - start;
        
        let validCount = 0;
        allParams.forEach(param => {
            if (history[param]) {
                for (let j = start; j < end; j++) {
                    if (history[param][j] != null) validCount++;
                }
            }
        });
        
        return (validCount / (window * allParams.length)) * 100;
    });
}

function updateDailyStatisticsChart(history) {
    if (!charts.dailyStatsChart) return;
    
    // Group data by day and calculate min, max, avg
    const dailyStats = {};
    const timestamps = history.timestamps || [];
    const pm25Data = history.pm2_5 || [];
    
    timestamps.forEach((timestamp, i) => {
        const date = new Date(timestamp).toDateString();
        if (!dailyStats[date]) {
            dailyStats[date] = [];
        }
        if (pm25Data[i] != null) {
            dailyStats[date].push(pm25Data[i]);
        }
    });
    
    const labels = Object.keys(dailyStats).slice(-7); // Last 7 days
    const minValues = labels.map(date => Math.min(...dailyStats[date]));
    const maxValues = labels.map(date => Math.max(...dailyStats[date]));
    const avgValues = labels.map(date => 
        dailyStats[date].reduce((a, b) => a + b, 0) / dailyStats[date].length
    );
    
    charts.dailyStatsChart.data.labels = labels.map(date => new Date(date).toLocaleDateString());
    charts.dailyStatsChart.data.datasets[0].data = minValues;
    charts.dailyStatsChart.data.datasets[1].data = avgValues;
    charts.dailyStatsChart.data.datasets[2].data = maxValues;
    const allValues = minValues.concat(avgValues).concat(maxValues);
    applyRigidAxisForBar('dailyStatsChart', charts.dailyStatsChart, allValues);
    safeChartUpdate(charts.dailyStatsChart, 'dailyStatsChart');
}

// ========================
// RIGID AXIS HELPERS AND HISTOGRAM
// ========================
function applyRigidAxisForLine(key, chart, arrays, yMin = 0, yMaxFixed = null) {
    if (!chart || !chart.options || !chart.options.scales) return;
    const flat = (arrays || []).flat().filter(v => Number.isFinite(v));
    const maxVal = flat.length ? Math.max(...flat) : 0;
    if (!rigidMaxByChart[key]) rigidMaxByChart[key] = {};
    const prev = rigidMaxByChart[key].y || 0;
    const newMax = yMaxFixed != null ? yMaxFixed : Math.max(prev, Math.ceil(maxVal * 1.1));
    rigidMaxByChart[key].y = newMax;
    chart.options.scales.y = chart.options.scales.y || {};
    chart.options.scales.y.min = yMin;
    chart.options.scales.y.max = Math.max(10, newMax);
    chart.options.scales.y.suggestedMax = undefined;
}

function applyRigidAxisForDual(key, chart, yArr, y1Arr, yMin = 0, y1Min = 0) {
    if (!chart || !chart.options || !chart.options.scales) return;
    if (!rigidMaxByChart[key]) rigidMaxByChart[key] = {};
    const yVals = (yArr || []).filter(Number.isFinite);
    const y1Vals = (y1Arr || []).filter(Number.isFinite);
    const yMax = yVals.length ? Math.ceil(Math.max(...yVals) * 1.1) : 0;
    const y1Max = y1Vals.length ? Math.ceil(Math.max(...y1Vals) * 1.1) : 0;
    rigidMaxByChart[key].y = Math.max(rigidMaxByChart[key].y || 0, yMax);
    rigidMaxByChart[key].y1 = Math.max(rigidMaxByChart[key].y1 || 0, y1Max);
    chart.options.scales.y = chart.options.scales.y || {};
    chart.options.scales.y1 = chart.options.scales.y1 || {};
    chart.options.scales.y.min = yMin;
    chart.options.scales.y.max = Math.max(10, rigidMaxByChart[key].y);
    chart.options.scales.y.suggestedMax = undefined;
    chart.options.scales.y1.min = y1Min;
    chart.options.scales.y1.max = Math.max(10, rigidMaxByChart[key].y1);
    chart.options.scales.y1.suggestedMax = undefined;
}

function applyRigidAxisForBar(key, chart, values, yMin = 0, yMaxFixed = null) {
    if (!chart || !chart.options || !chart.options.scales) return;
    const vals = (values || []).filter(Number.isFinite);
    const maxVal = vals.length ? Math.max(...vals) : 0;
    if (!rigidMaxByChart[key]) rigidMaxByChart[key] = {};
    const prev = rigidMaxByChart[key].y || 0;
    const newMax = yMaxFixed != null ? yMaxFixed : Math.max(prev, Math.ceil(maxVal * 1.1));
    rigidMaxByChart[key].y = newMax;
    chart.options.scales.y = chart.options.scales.y || {};
    chart.options.scales.y.min = yMin;
    chart.options.scales.y.max = Math.max(10, newMax);
    chart.options.scales.y.suggestedMax = undefined;
}

function applyRigidAxisForScatter(key, chart, xArray, yArray) {
    if (!chart || !chart.options || !chart.options.scales) return;
    if (!rigidMaxByChart[key]) rigidMaxByChart[key] = {};
    const xVals = (xArray || []).filter(Number.isFinite);
    const yVals = (yArray || []).filter(Number.isFinite);
    if (xVals.length) {
        const xMin = Math.min(...xVals);
        const xMax = Math.max(...xVals);
        rigidMaxByChart[key].xMin = rigidMaxByChart[key].xMin == null ? xMin : Math.min(rigidMaxByChart[key].xMin, xMin);
        rigidMaxByChart[key].xMax = rigidMaxByChart[key].xMax == null ? xMax : Math.max(rigidMaxByChart[key].xMax, xMax);
        chart.options.scales.x = chart.options.scales.x || {};
        chart.options.scales.x.min = rigidMaxByChart[key].xMin;
        chart.options.scales.x.max = rigidMaxByChart[key].xMax;
    }
    if (yVals.length) {
        const yMin = Math.min(...yVals);
        const yMax = Math.max(...yVals);
        rigidMaxByChart[key].yMin = rigidMaxByChart[key].yMin == null ? yMin : Math.min(rigidMaxByChart[key].yMin, yMin);
        rigidMaxByChart[key].y = Math.max(rigidMaxByChart[key].y || 0, Math.ceil(yMax * 1.1));
        chart.options.scales.y = chart.options.scales.y || {};
        chart.options.scales.y.min = rigidMaxByChart[key].yMin;
        chart.options.scales.y.max = Math.max(10, rigidMaxByChart[key].y);
    }
}

function buildHistogram(series, binCount = 20) {
    const arr = (series || []).filter(Number.isFinite);
    if (!arr.length) return { labels: [], counts: [] };
    const min = Math.min(...arr);
    const max = Math.max(...arr);
    const width = (max - min) || 1;
    const step = width / binCount;
    const edges = Array.from({ length: binCount + 1 }, (_, i) => min + i * step);
    const counts = Array(binCount).fill(0);
    arr.forEach(v => {
        const idx = Math.min(binCount - 1, Math.floor((v - min) / step));
        counts[idx]++;
    });
    const labels = Array.from({ length: binCount }, (_, i) => `${edges[i].toFixed(1)}–${edges[i+1].toFixed(1)}`);
    return { labels, counts };
}

// ========================
// EXTENDED DATA HANDLING (New)
// ========================

function updateExtendedData(extendedData) {
    console.log('🔄 updateExtendedData called with:', extendedData);

    if (!extendedData) {
        console.log('❌ No extended data provided');
        return;
    }

    // Debug: Log the structure of extendedData
    console.log('📊 Extended data structure:', Object.keys(extendedData));
    console.log('🌡️ Temperature:', extendedData.temperature_c);
    console.log('💧 Humidity:', extendedData.humidity_percent);
    console.log('🌪️ Pressure:', extendedData.pressure_hpa);
    console.log('🧪 VOC:', extendedData.voc_ppb);
    console.log('💡 Lux:', extendedData.lux);
    console.log('☀️ UV Index:', extendedData.uv_index);

    // Force activate environmental tab if not already active
    const environmentalTab = document.getElementById('environmental-tab');
    const environmentalTabContent = document.getElementById('environmental');

    if (environmentalTab && environmentalTabContent) {
        // Check if tab is hidden and show it
        if (environmentalTab.style.display === 'none') {
            console.log('🔧 Environmental tab was hidden, showing it now');
            environmentalTab.style.display = 'block';

            // Use Bootstrap's Tab API to properly activate
            const bsTab = new bootstrap.Tab(environmentalTab);
            bsTab.show();
        }

        console.log('✅ Environmental tab is visible and active');
    }

    // Re-create/re-render environmental cards dynamically
    console.log('🔄 Re-creating environmental cards dynamically...');
    createEnvironmentalDataCards(extendedData, {});

    // Update environmental summary in the overview tab
    updateEnvironmentalSummary(extendedData);

    // Update Google Maps coordinates if coordinates are available
    if (extendedData.gps_lat !== undefined && extendedData.gps_lon !== undefined && extendedData.gps_lat !== null && extendedData.gps_lon !== null) {
        console.log('📍 Updating GPS location on Google Map:', extendedData.gps_lat, extendedData.gps_lon);
        updateGoogleMapLocation(currentDeviceId, extendedData.gps_lat, extendedData.gps_lon);
    }
}

function updateEnvironmentalCard(elementId, value, unit) {
    console.log(`updateEnvironmentalCard: ${elementId}=${value}${unit}`);
    const element = document.getElementById(elementId);

    if (element) {
        console.log(`Found element ${elementId}, current text: "${element.textContent}"`);
        if (value !== null && value !== undefined && !isNaN(value)) {
            const newText = `${value.toFixed(1)}${unit}`;
            console.log(`Setting ${elementId} to: "${newText}"`);
            element.textContent = newText;
            element.style.color = '#0d6efd'; // Blue color for valid data
        } else {
            const newText = `--${unit}`;
            console.log(`Setting ${elementId} to: "${newText}" (no data)`);
            element.textContent = newText;
            element.style.color = '#6c757d'; // Gray color for no data
        }
        console.log(`Final text for ${elementId}: "${element.textContent}"`);
    } else {
        console.error(`Element with ID '${elementId}' not found!`);
        // List all elements with similar IDs to help debug
        const allElements = document.querySelectorAll('[id*="current"]');
        console.log('Available elements with "current" in ID:', Array.from(allElements).map(el => el.id));
    }
}

function updateEnvironmentalSummary(data) {
    const asDisplay = (value, suffix = '') => {
        return (typeof value === 'number' && !Number.isNaN(value)) ? `${value.toFixed(1)}${suffix}` : '--';
    };

    // Update temperature
    const tempElement = document.querySelector('#environmentalSummary .col-6:nth-child(1) .h6');
    if (tempElement) {
        tempElement.textContent = asDisplay(data.temperature_c, '°C');
    }

    // Update humidity
    const humidityElement = document.querySelector('#environmentalSummary .col-6:nth-child(2) .h6');
    if (humidityElement) {
        humidityElement.textContent = asDisplay(data.humidity_percent, '%');
    }

    // Update VOC
    const vocElement = document.querySelector('#environmentalSummary .col-6:nth-child(3) .h6');
    if (vocElement) {
        vocElement.textContent = asDisplay(data.voc_ppb, ' ppb');
    }

    // Update NO2
    const no2Element = document.querySelector('#environmentalSummary .col-6:nth-child(4) .h6');
    if (no2Element) {
        no2Element.textContent = asDisplay(data.no2_ppb, ' ppb');
    }
}

/**
 * Display MQTT data by feeding it to the existing timeChart graph
 */
function displayDynamicMQTTData(mqttData) {
    console.log('📊 Displaying MQTT data to timeChart:', mqttData);
    
    if (!mqttData || !mqttData.raw_data) return;
    
    const rawData = mqttData.raw_data || {};
    const timestamp = new Date(rawData.ts || rawData.t || rawData.timestamp || Date.now());
    
    // Extract MQTT PM values
    const getNumber = (...values) => {
        for (const value of values) {
            const parsed = Number(value);
            if (Number.isFinite(parsed)) return parsed;
        }
        return null;
    };
    
    const pm1 = getNumber(rawData.tsi_pm1);
    const pm25 = getNumber(rawData.tsi_pm25);
    const pm4 = getNumber(rawData.tsi_pm4);
    const pm10 = getNumber(rawData.tsi_pm10);
    
    // Feed data into the existing timeChart
    if (charts.timeChart) {
        charts.timeChart.data.labels = []; // Keep empty
        if (!charts.timeChart.data.datasets[0].data) charts.timeChart.data.datasets[0].data = [];
        if (!charts.timeChart.data.datasets[1].data) charts.timeChart.data.datasets[1].data = [];
        if (!charts.timeChart.data.datasets[2].data) charts.timeChart.data.datasets[2].data = [];
        if (!charts.timeChart.data.datasets[3].data) charts.timeChart.data.datasets[3].data = [];
        
        // Add new data point
        charts.timeChart.data.datasets[0].data.push({ x: timestamp, y: pm1 });
        charts.timeChart.data.datasets[1].data.push({ x: timestamp, y: pm25 });
        charts.timeChart.data.datasets[2].data.push({ x: timestamp, y: pm4 });
        charts.timeChart.data.datasets[3].data.push({ x: timestamp, y: pm10 });
        
        // Keep only last 30 points
        const maxPoints = 30;
        if (charts.timeChart.data.datasets[0].data.length > maxPoints) {
            charts.timeChart.data.datasets.forEach(ds => ds.data.shift());
        }
        
        safeChartUpdate(charts.timeChart, 'timeChart');
        console.log('✅ MQTT data added to timeChart');
    }
}

// ========================
// MISSING CORE FUNCTIONS
// ========================

function joinDeviceRoom(deviceId) {
    if (socket && socket.connected) {
        // Leave previous room
        if (currentDeviceRoom && currentDeviceRoom !== deviceId) {
            socket.emit('leave', { device_id: currentDeviceRoom , user_id: JSON.parse(localStorage.getItem('user_data') || '{}').id });
        }
        
        // Join new room
        socket.emit('join', { device_id: deviceId , user_id: JSON.parse(localStorage.getItem('user_data') || '{}').id });
        currentDeviceRoom = deviceId;
        console.log(`Joined room for device: ${deviceId}`);
    }
}

function updateConnectionStatus(isConnected) {
    const connectionElements = [
        document.getElementById('connectionStatus'),
        document.getElementById('deviceStatus'),
        document.getElementById('navConnectionStatus')
    ];
    
    const connectionDot = document.getElementById('connectionDot');
    
    connectionElements.forEach(el => {
        if (el) {
            el.textContent = isConnected ? 'Online' : 'Offline';
            el.className = `badge ${isConnected ? 'bg-success' : 'bg-danger'}`;
        }
    });
    
    if (connectionDot) {
        connectionDot.className = `status-dot ${isConnected ? 'online' : 'offline'}`;
    }
}

function showLoading() {
    const loadingDiv = document.createElement('div');
    loadingDiv.innerHTML = `
        <div class="d-flex justify-content-center align-items-center" style="position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.3); z-index: 9999;">
            <div class="spinner-border text-primary" role="status">
                <span class="visually-hidden">Loading...</span>
            </div>
        </div>
    `;
    document.body.appendChild(loadingDiv);
    return loadingDiv;
}

function createAlert(message, type = 'info') {
    const alertDiv = document.createElement('div');
    alertDiv.className = `alert alert-${type} alert-dismissible fade show position-fixed`;
    alertDiv.style.cssText = 'top: 20px; right: 20px; z-index: 9999; min-width: 300px;';
    alertDiv.innerHTML = `
        ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
    `;
    document.body.appendChild(alertDiv);
    
    // Auto remove after 5 seconds
    setTimeout(() => {
        if (alertDiv.parentNode) {
            alertDiv.remove();
        }
    }, 5000);
}

function setDefaultDates() {
    const endDate = new Date();
    const startDate = new Date(endDate.getTime() - 24 * 60 * 60 * 1000); // 24 hours ago
    
    const startDateInput = document.getElementById('startDate');
    const endDateInput = document.getElementById('endDate');
    
    if (startDateInput) {
        startDateInput.value = startDate.toISOString().split('T')[0];
    }
    if (endDateInput) {
        endDateInput.value = endDate.toISOString().split('T');
    }
}

function setupRelayControls() {
    // Relay ON button
    const relayOnBtn = document.getElementById('relayOnBtn');
    if (relayOnBtn) {
        relayOnBtn.addEventListener('click', function() {
            if (!currentDeviceId) {
                createAlert('Please select a device first', 'warning');
                return;
            }
            
            apiFetch('/api/relay_control', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ device_id: currentDeviceId, state: 'ON' })
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    createAlert('Relay turned ON', 'success');
                } else {
                    createAlert(data.message || 'Failed to turn ON relay', 'danger');
                }
            })
            .catch(error => {
                console.error('Error controlling relay:', error);
                createAlert('Error controlling relay', 'danger');
            });
        });
    }
    
    // Relay OFF button
    const relayOffBtn = document.getElementById('relayOffBtn');
    if (relayOffBtn) {
        relayOffBtn.addEventListener('click', function() {
            if (!currentDeviceId) {
                createAlert('Please select a device first', 'warning');
                return;
            }
            
            apiFetch('/api/relay_control', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ device_id: currentDeviceId, state: 'OFF' })
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    createAlert('Relay turned OFF', 'success');
                } else {
                    createAlert(data.message || 'Failed to turn OFF relay', 'danger');
                }
            })
            .catch(error => {
                console.error('Error controlling relay:', error);
                createAlert('Error controlling relay', 'danger');
            });
        });
    }
}

// Initialize map for location tracking
function initializeMap() {
    const mapContainer = document.getElementById('deviceMap');
    if (!mapContainer || map) return;
    
    map = L.map('deviceMap').setView([20.5937, 78.9629], 5);
    L.tileLayer('https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png', {
        attribution: '© OpenStreetMap contributors © CARTO'
    }).addTo(map);
    
    deviceMarker = L.marker([20.5937, 78.9629]).addTo(map);
    
    console.log('Map initialized for extended device');
}


// ========================
// QUICK STATS & AQI (New)
// ========================

function updateQuickStats(data) {
    // Update export count badge
    const exportCount = document.getElementById('exportCount');
    if (exportCount && data && data.total_records !== undefined) {
        exportCount.textContent = `${data.total_records} records`;
    }
    
    // Update cloud cover
    const cloudCover = document.getElementById('cloudCover');
    if (cloudCover && data && data.extended && typeof data.extended.cloud_cover_percent === 'number') {
        cloudCover.textContent = `${data.extended.cloud_cover_percent.toFixed(1)}%`;
    }
    
    // Update noise level
    const noiseLevel = document.getElementById('noiseLevel');
    if (noiseLevel && data && data.extended) {
        const noiseVal = data.extended.noise_db !== undefined && data.extended.noise_db !== null ? data.extended.noise_db : data.extended.sound;
        if (typeof noiseVal === 'number') {
            noiseLevel.textContent = `${noiseVal.toFixed(1)} dB`;
        } else {
            noiseLevel.textContent = '-- dB';
        }
    }
}

function updateAQI(data) {
    console.log('updateAQI called with data:', data);

    // Check if backend AQI data is available
    if (data.aqi && data.aqi.current) {
        console.log('Using AQI data from backend:', data.aqi);
        updateAQIFromBackend(data.aqi);
        return;
    }

    // Fallback: Calculate AQI from PM2.5 if backend data not available
    console.log('Using fallback AQI calculation from PM data');
    
    const aqiCircle = document.getElementById('aqiCircle');
    const aqiValue = document.getElementById('aqiValue');
    const aqiLabel = document.getElementById('aqiLabel');
    const aqiDescription = document.getElementById('aqiDescription');

    // Use PM2.5 from sensor data (primary) or extended data (fallback)
    let pm25 = null;

    // Check sensor data first
    if (data.sensor && typeof data.sensor.pm2_5 === 'number' && !isNaN(data.sensor.pm2_5)) {
        pm25 = data.sensor.pm2_5;
        console.log('Using PM2.5 from sensor data:', pm25);
    }
    // Check extended data as fallback
    else if (data.extended && typeof data.extended.pm2_5 === 'number' && !isNaN(data.extended.pm2_5)) {
        pm25 = data.extended.pm2_5;
        console.log('Using PM2.5 from extended data:', pm25);
    }

    if (pm25 === null || pm25 < 0) {
        console.warn("AQI: No valid PM2.5 data available");
        if (aqiValue) aqiValue.textContent = '--';
        if (aqiLabel) aqiLabel.textContent = 'No Data';
        if (aqiDescription) aqiDescription.textContent = 'Select a device to see AQI';
        if (aqiCircle) aqiCircle.style.borderColor = '#e9ecef';
        return;
    }

    console.log('Calculating AQI for PM2.5:', pm25);

    let aqi, label, description, color;

    if (pm25 <= 12) {
        aqi = Math.round((50 / 12) * pm25);
        label = 'Good';
        description = 'Air quality is satisfactory';
        color = '#00e400';
    } else if (pm25 <= 35.4) {
        aqi = Math.round(51 + ((100 - 51) / (35.4 - 12.1)) * (pm25 - 12.1));
        label = 'Moderate';
        description = 'Air quality is acceptable for most';
        color = '#ffff00';
    } else if (pm25 <= 55.4) {
        aqi = Math.round(101 + ((150 - 101) / (55.4 - 35.5)) * (pm25 - 35.5));
        label = 'Unhealthy for Sensitive';
        description = 'Sensitive individuals may experience symptoms';
        color = '#ff7e00';
    } else if (pm25 <= 150.4) {
        aqi = Math.round(151 + ((200 - 151) / (150.4 - 55.5)) * (pm25 - 55.5));
        label = 'Unhealthy';
        description = 'Everyone may experience symptoms';
        color = '#ff0000';
    } else {
        aqi = Math.round(201 + ((300 - 201) / (250.4 - 150.5)) * (pm25 - 150.5));
        label = 'Very Unhealthy';
        description = 'Health warnings of emergency conditions';
        color = '#8f3f97';
    }

    console.log('Calculated AQI:', { aqi, label, description, color });

    // Update AQI display
    if (aqiValue) {
        aqiValue.textContent = aqi;
        aqiValue.style.color = color;
    }
    if (aqiLabel) {
        aqiLabel.textContent = label;
        aqiLabel.style.color = color;
    }
    if (aqiDescription) {
        aqiDescription.textContent = description;
    }
    if (aqiCircle) {
        aqiCircle.style.borderColor = color;
    }
}

/**
 * Update AQI display using data from backend
 * Backend AQI format: { current: {...}, average: {...} }
 */
function updateAQIFromBackend(aqiData) {
    try {
        const current = aqiData.current || {};
        const average = aqiData.average || {};

        console.log('Updating AQI display from backend:', { current, average });

        // Update quick stat card
        const aqiIcon = document.getElementById('aqiIcon');
        const aqiIndex = document.getElementById('aqiIndex');
        const aqiLevel = document.getElementById('aqiLevel');

        if (aqiIcon) {
            aqiIcon.style.backgroundColor = current.color || '#808080';
        }
        if (aqiIndex) {
            aqiIndex.textContent = current.index !== undefined ? current.index : '--';
        }
        if (aqiLevel) {
            aqiLevel.textContent = (current.level || 'Unknown').toUpperCase();
        }

        // Update visible AQI card elements in current template
        const aqiValue = document.getElementById('aqiValue');
        const aqiLabel = document.getElementById('aqiLabel');
        if (aqiValue) {
            aqiValue.textContent = current.index !== undefined ? current.index : '--';
            aqiValue.style.color = current.color || '#6c757d';
        }
        if (aqiLabel) {
            aqiLabel.textContent = current.level || 'No Data';
            aqiLabel.style.color = current.color || '#6c757d';
        }

        // Update main AQI section
        const aqiCircle = document.getElementById('aqiCircle');
        const aqiIndexMain = document.getElementById('aqiIndexMain');
        const aqiLevelMain = document.getElementById('aqiLevelMain');
        const aqiDescription = document.getElementById('aqiDescription');
        const aqiPM25Main = document.getElementById('aqiPM25Main');
        const aqiPM10Main = document.getElementById('aqiPM10Main');
        const aqiAverageMain = document.getElementById('aqiAverageMain');

        // Update circle with border color
        if (aqiCircle) {
            aqiCircle.style.borderColor = current.color || '#ddd';
            aqiCircle.style.background = `linear-gradient(135deg, ${current.color || '#f0f0f0'}20 0%, #ffffff 100%)`;
        }

        // Update main index
        if (aqiIndexMain) {
            aqiIndexMain.textContent = current.index !== undefined ? current.index : '--';
            aqiIndexMain.style.color = current.color || '#333';
        }

        // Update level
        if (aqiLevelMain) {
            aqiLevelMain.textContent = current.level || 'Unknown';
            aqiLevelMain.style.color = current.color || '#333';
        }

        // Update description
        if (aqiDescription) {
            const descriptions = {
                'Low': 'Air quality is satisfactory and pollution poses little or no risk.',
                'Moderate': 'Air quality is acceptable for most people, but there may be risk for some.',
                'High': 'Some groups such as children, the elderly, and people with respiratory disease are more likely to be affected.',
                'Very High': 'Health alert: The general public is more likely to be affected.'
            };
            aqiDescription.textContent = descriptions[current.level] || `AQI Level: ${current.level || 'Unknown'}`;
        }

        // Update PM values
        if (aqiPM25Main) {
            aqiPM25Main.textContent = current.pm2_5 !== undefined ? current.pm2_5.toFixed(1) : '--';
        }
        if (aqiPM10Main) {
            aqiPM10Main.textContent = current.pm10 !== undefined ? current.pm10.toFixed(1) : '--';
        }

        // Update average AQI
        if (aqiAverageMain) {
            const avgLevel = average.level || 'Unknown';
            const avgIndex = average.index !== undefined ? average.index : '--';
            aqiAverageMain.innerHTML = `${avgIndex} <small style="color: #666;">(${avgLevel})</small>`;
            if (average.color) {
                aqiAverageMain.style.color = average.color;
            }
        }

        console.log('✅ AQI display updated successfully');
    } catch (error) {
        console.error('❌ Error updating AQI from backend:', error);
    }
}




// ========================
// MISSING CORE FUNCTIONS
// ========================

function updateQuickStats(data) {
    if (!data || !data.sensor) return;

    const toDisplayValue = (value) => {
        return (typeof value === 'number' && !Number.isNaN(value)) ? value.toFixed(1) : '--';
    };
    
    // Update quick stats cards
    const statsElements = {
        'quickPM25': data.sensor.pm2_5,
        'quickPM10': data.sensor.pm10,
        'quickTemp': data.extended?.temperature_c,
        'quickHumidity': data.extended?.humidity_percent
    };
    
    Object.entries(statsElements).forEach(([id, value]) => {
        const element = document.getElementById(id);
        if (element) {
            element.textContent = toDisplayValue(value);
        }
    });
}



function checkThresholds(data) {
    if (!data || !data.sensor || !data.status?.thresholds) return;
    
    const sensor = data.sensor;
    const thresholds = data.status.thresholds;
    
    const exceedances = [];
    
    if (sensor.pm1 > thresholds.pm1) exceedances.push('PM1');
    if (sensor.pm2_5 > thresholds['pm2.5']) exceedances.push('PM2.5');
    if (sensor.pm4 > thresholds.pm4) exceedances.push('PM4');
    if (sensor.pm10 > thresholds.pm10) exceedances.push('PM10');
    if (sensor.tsp > thresholds.tsp) exceedances.push('TSP');
    
    if (exceedances.length > 0) {
        createAlert(`Threshold exceeded for: ${exceedances.join(', ')}`, 'warning');
    }
}

function calculateThresholdFrequency(data) {
    if (!data || !data.history) return;
    
    // This would normally calculate from historical data
    // For now, just update the chart with sample data
    if (charts.thresholdFrequencyChart) {
        const sampleData = [10, 15, 8, 12, 5]; // Sample percentages
        charts.thresholdFrequencyChart.data.datasets[0].data = sampleData;
        safeChartUpdate(charts.thresholdFrequencyChart, 'thresholdFrequency');
    }
}



function updateDeepAnalyticsCharts(data) {
    // Placeholder for deep analytics updates
    console.log('Deep analytics charts updated');
}

function getParameterLabel(param) {
    const labels = {
        'voc': 'VOC (ppb)',
        'no2': 'NO₂ (ppb)',
        'speed': 'Speed (km/h)',
        'temp': 'Temperature (°C)',
        'humidity': 'Humidity (%)',
        'pressure': 'Pressure (hPa)'
    };
    return labels[param] || param.toUpperCase();
}

// Shadowed updateCharts function removed and consolidated to avoid clashing declarations

function calculateAQI(pm25) {
    // CPCB (India) PM2.5 AQI calculation
    if (pm25 === null || pm25 === undefined || pm25 === '') return null;
    const c = Math.round(Number(pm25));
    
    const interpolate = (val, cLow, cHigh, iLow, iHigh) => {
        return Math.round(((iHigh - iLow) / (cHigh - cLow)) * (val - cLow) + iLow);
    };

    if (c <= 30)  return interpolate(c, 0, 30, 0, 50);
    if (c <= 60)  return interpolate(c, 31, 60, 51, 100);
    if (c <= 90)  return interpolate(c, 61, 90, 101, 200);
    if (c <= 120) return interpolate(c, 91, 120, 201, 300);
    if (c <= 250) return interpolate(c, 121, 250, 301, 400);
    if (c <= 380) return interpolate(c, 251, 380, 401, 500);
    return 500; // Cap at Severe index maximum 500
}

function getAQIHexColor(aqi) {
    // CPCB (India) AQI color bands
    if (aqi <= 50) return '#00B050';  // Good - Green
    if (aqi <= 100) return '#92D050'; // Satisfactory - Light Green
    if (aqi <= 200) return '#FFFF00'; // Moderately Polluted - Yellow
    if (aqi <= 300) return '#FFC000'; // Poor - Orange
    if (aqi <= 400) return '#FF0000'; // Very Poor - Red
    return '#C00000';                 // Severe - Maroon
}

function updateExtendedCharts(data) {
    if (!data) return;

    console.log('updateExtendedCharts called with:', data);

    if (!charts.environmentalCombinedChart) {
        return;
    }

    const pmTimestamps = data.history && data.history.timestamps ? data.history.timestamps : [];
    const extHistory = data.history && data.history.extended ? data.history.extended : {};
    const extTimestamps = extHistory.timestamps || [];
    
    // Normalize and parse timestamps safely using parseLocalTime
    const parseTime = (t) => {
        return parseLocalTime(t);
    };

    // 1. Sort standard PM data chronologically
    const pmPoints = [];
    for (let i = 0; i < pmTimestamps.length; i++) {
        const d = parseTime(pmTimestamps[i]);
        if (d) {
            pmPoints.push({
                date: d,
                pm1: data.history.pm1 ? data.history.pm1[i] : null,
                pm2_5: data.history.pm2_5 ? data.history.pm2_5[i] : null,
                pm10: data.history.pm10 ? data.history.pm10[i] : null
            });
        }
    }
    pmPoints.sort((a, b) => a.date.getTime() - b.date.getTime());
    
    const sortedPmTimestamps = pmPoints.map(p => p.date);
    const sortedPm1 = pmPoints.map(p => p.pm1);
    const sortedPm2_5 = pmPoints.map(p => p.pm2_5);
    const sortedPm10 = pmPoints.map(p => p.pm10);

    // 2. Sort extended environmental data chronologically
    const extPoints = [];
    for (let i = 0; i < extTimestamps.length; i++) {
        const d = parseTime(extTimestamps[i]);
        if (d) {
            extPoints.push({
                date: d,
                temperature_c: extHistory.temperature_c ? extHistory.temperature_c[i] : null,
                humidity_percent: extHistory.humidity_percent ? extHistory.humidity_percent[i] : null,
                pressure_hpa: extHistory.pressure_hpa ? extHistory.pressure_hpa[i] : null,
                voc_ppb: extHistory.voc_ppb ? extHistory.voc_ppb[i] : null,
                no2_ppb: extHistory.no2_ppb ? extHistory.no2_ppb[i] : null,
                noise_db: extHistory.noise_db ? extHistory.noise_db[i] : null
            });
        }
    }
    extPoints.sort((a, b) => a.date.getTime() - b.date.getTime());
    
    const sortedExtTimestamps = extPoints.map(p => p.date);
    const sortedTemp = extPoints.map(p => p.temperature_c);
    const sortedHumid = extPoints.map(p => p.humidity_percent);
    const sortedPressure = extPoints.map(p => p.pressure_hpa);
    const sortedVoc = extPoints.map(p => p.voc_ppb);
    const sortedNo2 = extPoints.map(p => p.no2_ppb);
    const sortedNoise = extPoints.map(p => p.noise_db);

    // 3. Merge standard and extended timelines for a combined time-series chart
    const combinedUniqueTimes = new Set();
    sortedPmTimestamps.forEach(d => combinedUniqueTimes.add(d.getTime()));
    sortedExtTimestamps.forEach(d => combinedUniqueTimes.add(d.getTime()));
    
    const sortedCombinedTimes = Array.from(combinedUniqueTimes).sort((a, b) => a - b);
    const sortedCombinedTimestamps = sortedCombinedTimes.map(t => new Date(t));
    
    // Create maps for alignment
    const pmMap = { pm1: new Map(), pm2_5: new Map(), pm10: new Map() };
    for (let i = 0; i < sortedPmTimestamps.length; i++) {
        const timeMs = sortedPmTimestamps[i].getTime();
        if (sortedPm1[i] !== null && sortedPm1[i] !== undefined) pmMap.pm1.set(timeMs, sortedPm1[i]);
        if (sortedPm2_5[i] !== null && sortedPm2_5[i] !== undefined) pmMap.pm2_5.set(timeMs, sortedPm2_5[i]);
        if (sortedPm10[i] !== null && sortedPm10[i] !== undefined) pmMap.pm10.set(timeMs, sortedPm10[i]);
    }
    
    const extMap = {
        temp: new Map(), humid: new Map(), pressure: new Map(),
        voc: new Map(), no2: new Map(), noise: new Map()
    };
    for (let i = 0; i < sortedExtTimestamps.length; i++) {
        const timeMs = sortedExtTimestamps[i].getTime();
        if (sortedTemp[i] !== null && sortedTemp[i] !== undefined) extMap.temp.set(timeMs, sortedTemp[i]);
        if (sortedHumid[i] !== null && sortedHumid[i] !== undefined) extMap.humid.set(timeMs, sortedHumid[i]);
        if (sortedPressure[i] !== null && sortedPressure[i] !== undefined) extMap.pressure.set(timeMs, sortedPressure[i]);
        if (sortedVoc[i] !== null && sortedVoc[i] !== undefined) extMap.voc.set(timeMs, sortedVoc[i]);
        if (sortedNo2[i] !== null && sortedNo2[i] !== undefined) extMap.no2.set(timeMs, sortedNo2[i]);
        if (sortedNoise[i] !== null && sortedNoise[i] !== undefined) extMap.noise.set(timeMs, sortedNoise[i]);
    }
    
    // Map mapped values to combined timeline
    const alignedPm1 = sortedCombinedTimes.map(t => pmMap.pm1.has(t) ? Number(pmMap.pm1.get(t)) : null);
    const alignedPm2_5 = sortedCombinedTimes.map(t => pmMap.pm2_5.has(t) ? Number(pmMap.pm2_5.get(t)) : null);
    const alignedPm10 = sortedCombinedTimes.map(t => pmMap.pm10.has(t) ? Number(pmMap.pm10.get(t)) : null);
    
    const alignedTemp = sortedCombinedTimes.map(t => extMap.temp.has(t) ? Number(extMap.temp.get(t)) : null);
    const alignedHumid = sortedCombinedTimes.map(t => extMap.humid.has(t) ? Number(extMap.humid.get(t)) : null);
    const alignedPressure = sortedCombinedTimes.map(t => extMap.pressure.has(t) ? Number(extMap.pressure.get(t)) : null);
    const alignedVoc = sortedCombinedTimes.map(t => extMap.voc.has(t) ? Number(extMap.voc.get(t)) : null);
    const alignedNo2 = sortedCombinedTimes.map(t => extMap.no2.has(t) ? Number(extMap.no2.get(t)) : null);
    const alignedNoise = sortedCombinedTimes.map(t => extMap.noise.has(t) ? Number(extMap.noise.get(t)) : null);

    // If extended data doesn't have history but has a single current value, fallback to array filling
    const extendedData = data.extended || {};
    const finalTemp = alignedTemp.some(v => v !== null) ? alignedTemp : 
        (extendedData.temperature_c ? Array(sortedCombinedTimestamps.length).fill(Number(extendedData.temperature_c)) : []);
    const finalHumid = alignedHumid.some(v => v !== null) ? alignedHumid : 
        (extendedData.humidity_percent ? Array(sortedCombinedTimestamps.length).fill(Number(extendedData.humidity_percent)) : []);
    const finalPressure = alignedPressure.some(v => v !== null) ? alignedPressure : 
        (extendedData.pressure_hpa ? Array(sortedCombinedTimestamps.length).fill(Number(extendedData.pressure_hpa)) : []);
    const finalVoc = alignedVoc.some(v => v !== null) ? alignedVoc : 
        (extendedData.voc_ppb !== undefined ? Array(sortedCombinedTimestamps.length).fill(Number(extendedData.voc_ppb)) : []);
    const finalNo2 = alignedNo2.some(v => v !== null) ? alignedNo2 : 
        (extendedData.no2_ppb !== undefined ? Array(sortedCombinedTimestamps.length).fill(Number(extendedData.no2_ppb)) : []);
    const finalNoise = alignedNoise.some(v => v !== null) ? alignedNoise : 
        (extendedData.noise_db !== undefined ? Array(sortedCombinedTimestamps.length).fill(Number(extendedData.noise_db)) : 
         (extendedData.sound !== undefined ? Array(sortedCombinedTimestamps.length).fill(Number(extendedData.sound)) : []));

    charts.environmentalCombinedChart.data.labels = []; // Clear labels
    
    let targetDate = new Date();
    if (sortedCombinedTimestamps && sortedCombinedTimestamps.length > 0) {
        targetDate = new Date(sortedCombinedTimestamps[sortedCombinedTimestamps.length - 1]);
    }
    setChartDayBounds(charts.environmentalCombinedChart, targetDate);

    const datasetsConfig = [
        { label: 'PM1:', unit: 'μg/m³', data: alignedPm1, color: colorScheme.pm1 },
        { label: 'PM2.5:', unit: 'μg/m³', data: alignedPm2_5, color: colorScheme.pm2_5 },
        { label: 'PM10:', unit: 'μg/m³', data: alignedPm10, color: colorScheme.pm10 },
        { label: 'Temperature:', unit: '°C', data: finalTemp, color: colorScheme.temperature },
        { label: 'Humidity:', unit: '%', data: finalHumid, color: colorScheme.humidity },
        { label: 'Pressure:', unit: 'hPa', data: finalPressure, color: colorScheme.pressure },
        { label: 'VOC:', unit: 'ppb', data: finalVoc, color: colorScheme.voc },
        { label: 'NO2:', unit: 'ppb', data: finalNo2, color: colorScheme.no2 },
        { label: 'Noise:', unit: 'dB', data: finalNoise, color: colorScheme.noise }
    ];

    const dynamicLegend = document.getElementById('dynamicChartLegend');
    if (dynamicLegend) dynamicLegend.innerHTML = '';

    datasetsConfig.forEach((config, index) => {
        const hasData = config.data && config.data.some(v => v !== null && v !== undefined && v !== '');
        charts.environmentalCombinedChart.data.datasets[index].data = sortedCombinedTimestamps.map((t, idx) => ({ x: t, y: config.data[idx] }));
        if (charts.environmentalCombinedChart.data.datasets[index].hidden === undefined || charts.environmentalCombinedChart.data.datasets[index].hidden === null) {
            const hiddenByDefault = [0, 5, 6, 7, 8]; // PM1, Pressure, VOC, NO2, Noise
            charts.environmentalCombinedChart.data.datasets[index].hidden = !hasData || hiddenByDefault.includes(index);
        } else {
            if (!hasData) {
                charts.environmentalCombinedChart.data.datasets[index].hidden = true;
            }
        }

        if (hasData && dynamicLegend) {
            const lastVal = config.data.slice().reverse().find(v => v !== null && v !== undefined && v !== '');
            const displayVal = lastVal !== undefined ? Number(lastVal).toFixed(1) : '--';
            
            const col = document.createElement('div');
            col.className = 'col';
            col.innerHTML = `
                <div class="legend-item p-1">
                    <div class="legend-color" style="background: ${config.color.replace('0.8)', '1)')}"></div>
                    <span>${config.label} <strong style="color: ${chartTextColor}">${displayVal} ${config.unit}</strong></span>
                </div>
            `;
            dynamicLegend.appendChild(col);
        }
    });

    // Add AQI calculation
    const pm25Data = datasetsConfig[1].data;
    if (pm25Data && pm25Data.some(v => v !== null && v !== undefined && v !== '') && dynamicLegend) {
        const lastPm25Val = pm25Data.slice().reverse().find(v => v !== null && v !== undefined && v !== '');
        if (lastPm25Val !== undefined) {
            const aqi = calculateAQI(lastPm25Val);
            if (aqi !== null) {
                const aqiColor = getAQIHexColor(aqi);
                const col = document.createElement('div');
                col.className = 'col';
                col.innerHTML = `
                    <div class="legend-item p-1">
                        <div class="legend-color" style="background: ${aqiColor}"></div>
                        <span>AQI: <strong style="color: ${chartTextColor}">${aqi}</strong></span>
                    </div>
                `;
                dynamicLegend.appendChild(col);
            }
        }
    }

    safeChartUpdate(charts.environmentalCombinedChart, 'environmentalCombinedChart');
}

function updateIndividualExtendedCharts(data) {
    const extendedHistory = data.history && data.history.extended ? data.history.extended : {};
    const extTimestamps = extendedHistory.timestamps || [];
    if (extTimestamps.length === 0) return;

    // Safely parse and normalize timestamp representations
    const parseTime = (t) => {
        if (!t) return null;
        const s = typeof t === 'string' ? t.replace(' ', 'T') : t;
        const d = new Date(s);
        return isNaN(d.getTime()) ? null : d;
    };

    // Sort extended history data chronologically
    const extPoints = [];
    for (let i = 0; i < extTimestamps.length; i++) {
        const d = parseTime(extTimestamps[i]);
        if (d) {
            extPoints.push({
                date: d,
                voc_ppb: extendedHistory.voc_ppb ? extendedHistory.voc_ppb[i] : null,
                no2_ppb: extendedHistory.no2_ppb ? extendedHistory.no2_ppb[i] : null,
                noise_db: extendedHistory.noise_db ? extendedHistory.noise_db[i] : null,
                gps_speed_kmh: extendedHistory.gps_speed_kmh ? extendedHistory.gps_speed_kmh[i] : null
            });
        }
    }
    extPoints.sort((a, b) => a.date.getTime() - b.date.getTime());

    const sortedTimestamps = extPoints.map(p => p.date);
    const sortedVoc = extPoints.map(p => p.voc_ppb);
    const sortedNo2 = extPoints.map(p => p.no2_ppb);
    const sortedNoise = extPoints.map(p => p.noise_db);
    const sortedSpeed = extPoints.map(p => p.gps_speed_kmh);

    console.log('updateIndividualExtendedCharts - sorted timestamps length:', sortedTimestamps.length);

    // VOC Chart
    if (charts.vocChart) {
        const vocData = sortedVoc.filter(v => v !== null);
        if (sortedVoc.length > 0) {
            charts.vocChart.data.labels = sortedTimestamps;
            charts.vocChart.data.datasets[0].data = sortedVoc;
            safeChartUpdate(charts.vocChart, 'vocChart');
            console.log('Updated VOC chart with', sortedVoc.length, 'data points');
        }
    }

    // NO2 Chart
    if (charts.no2Chart) {
        const no2Data = sortedNo2.filter(v => v !== null);
        if (sortedNo2.length > 0) {
            charts.no2Chart.data.labels = sortedTimestamps;
            charts.no2Chart.data.datasets[0].data = sortedNo2;
            safeChartUpdate(charts.no2Chart, 'no2Chart');
            console.log('Updated NO2 chart with', sortedNo2.length, 'data points');
        }
    }

    // Noise Chart
    if (charts.noiseChart) {
        const noiseData = sortedNoise.filter(v => v !== null);
        if (sortedNoise.length > 0) {
            charts.noiseChart.data.labels = sortedTimestamps;
            charts.noiseChart.data.datasets[0].data = sortedNoise;
            safeChartUpdate(charts.noiseChart, 'noiseChart');
            console.log('Updated Noise chart with', sortedNoise.length, 'data points');
        }
    }

    // Speed Chart
    if (charts.speedChart) {
        const speedData = sortedSpeed.filter(v => v !== null);
        if (sortedSpeed.length > 0) {
            charts.speedChart.data.labels = sortedTimestamps;
            charts.speedChart.data.datasets[0].data = sortedSpeed;
            safeChartUpdate(charts.speedChart, 'speedChart');
            console.log('Updated Speed chart with', sortedSpeed.length, 'data points');
        }
    }
}

// ========================
// MAP TOGGLE FUNCTIONALITY  
// ========================
function initializeMapToggle() {
    const toggleMapBtn = document.getElementById('toggleMapView');
    const mapContainer = document.getElementById('mapSelectionContainer');
    const mapToggleIcon = document.getElementById('mapToggleIcon');
    const mapToggleText = document.getElementById('mapToggleText');
    
    if (!toggleMapBtn || !mapContainer) return;
    
    let mapVisible = false;
    
    toggleMapBtn.addEventListener('click', function() {
        mapVisible = !mapVisible;
        
        if (mapVisible) {
            mapContainer.style.display = 'block';
            mapToggleIcon.className = 'fas fa-times';
            mapToggleText.textContent = 'Hide Map';
            
            setTimeout(() => {
                if (deviceSelectMap) {
                    if (typeof deviceSelectMap.invalidateSize === 'function') {
                        deviceSelectMap.invalidateSize();
                        if (deviceSelectMarkers.length > 0) {
                            const latLngs = deviceSelectMarkers.map(m => m.getLatLng());
                            deviceSelectMap.fitBounds(latLngs);
                        }
                    } else if (typeof google !== 'undefined' && google.maps) {
                        google.maps.event.trigger(deviceSelectMap, 'resize');
                        
                        // Focus on active device and open its info window directly
                        if (currentDeviceId && deviceSelectMarkers.length > 0) {
                            const activeMarker = deviceSelectMarkers.find(m => String(m.deviceId) === String(currentDeviceId));
                            if (activeMarker) {
                                deviceSelectMap.panTo(activeMarker.getPosition());
                                if (activeMarker.infoWindow) {
                                    activeMarker.infoWindow.open(deviceSelectMap, activeMarker);
                                }
                                const lat = activeMarker.getPosition().lat();
                                const lng = activeMarker.getPosition().lng();
                                if ((Math.abs(lat - 70.0) < 1.0 && Math.abs(lng - 30.0) < 1.0) || (lat === 0.0 && lng === 0.0)) {
                                    deviceSelectMap.setZoom(4);
                                } else {
                                    deviceSelectMap.setZoom(8);
                                }
                                return;
                            }
                        }

                        if (deviceSelectMarkers.length > 0) {
                            const bounds = new google.maps.LatLngBounds();
                            deviceSelectMarkers.forEach(m => bounds.extend(m.getPosition()));
                            deviceSelectMap.fitBounds(bounds);
                            
                            if (deviceSelectMarkers.length === 1) {
                                google.maps.event.addListenerOnce(deviceSelectMap, 'idle', () => {
                                    const center = bounds.getCenter();
                                    const lat = center.lat();
                                    const lng = center.lng();
                                    if ((Math.abs(lat - 70.0) < 1.0 && Math.abs(lng - 30.0) < 1.0) || (lat === 0.0 && lng === 0.0)) {
                                        deviceSelectMap.setZoom(4);
                                    } else if (deviceSelectMap.getZoom() > 8) {
                                        deviceSelectMap.setZoom(8);
                                    }
                                });
                            }
                        }
                    }
                } else {
                    initializeDeviceSelectionMap();
                }
            }, 100);
        } else {
            mapContainer.style.display = 'none';
            mapToggleIcon.className = 'fas fa-map';
            mapToggleText.textContent = 'Show Map';
        }
    });
}

function initializeDeviceQuickActions() {
    const refreshBtn = document.getElementById('refreshDeviceList');
    const autoSelectBtn = document.getElementById('autoSelectDevice');
    
    if (refreshBtn) {
        refreshBtn.addEventListener('click', function() {
            window.location.reload();
        });
    }
    
    if (autoSelectBtn) {
        autoSelectBtn.addEventListener('click', function() {
            const deviceSelect = document.getElementById('deviceSelect');
            if (deviceSelect && deviceSelect.options.length > 1) {
                deviceSelect.selectedIndex = 1;
                deviceSelect.dispatchEvent(new Event('change'));
                createAlert('Auto-selected first available device', 'success');
            } else {
                createAlert('No devices available for auto-selection', 'warning');
            }
        });
    }
}




// ========================
// WEBSOCKET HANDLING (Enhanced)
// ========================

// (Replaced by the unified initializeWebSocket above)

function joinDeviceRoom(deviceId) {
    if (currentDeviceRoom && socket) {
        socket.emit('leave', { device_id: currentDeviceRoom , user_id: JSON.parse(localStorage.getItem('user_data') || '{}').id });
    }
    
    if (socket && deviceId) {
        socket.emit('join', { device_id: deviceId , user_id: JSON.parse(localStorage.getItem('user_data') || '{}').id });
        currentDeviceRoom = deviceId;
    }
}

function updateConnectionStatus(isOnline) {
    const connectionStatus = document.getElementById('connectionStatus') || document.getElementById('deviceStatus');
    const navConnectionStatus = document.getElementById('navConnectionStatus');
    const connectionDot = document.getElementById('connectionDot');
    
    if (connectionStatus) {
        connectionStatus.textContent = isOnline ? 'ONLINE' : 'OFFLINE';
        connectionStatus.className = `badge ${isOnline ? 'bg-success' : 'bg-danger'}`;
    }
    
    if (navConnectionStatus) {
        navConnectionStatus.textContent = isOnline ? 'Online' : 'Offline';
    }
    
    if (connectionDot) {
        connectionDot.className = `status-dot ${isOnline ? 'online' : ''}`;
    }
}

// ========================
// THRESHOLD MANAGEMENT (Your existing enhanced)
// ========================

function checkThresholds(data) {
    if (!data.sensor || !data.status || !data.status.thresholds) return;
    
    const alerts = [];
    const sensor = data.sensor;
    const thresholds = data.status.thresholds;
    
    const params = [
        { key: 'pm1', label: 'PM1', value: sensor.pm1 },
        { key: 'pm2.5', label: 'PM2.5', value: sensor.pm2_5 },
        { key: 'pm4', label: 'PM4', value: sensor.pm4 },
        { key: 'pm10', label: 'PM10', value: sensor.pm10 },
        { key: 'tsp', label: 'TSP', value: sensor.tsp }
    ];
    
    params.forEach(param => {
        const threshold = thresholds[param.key];
        if (param.value && threshold && param.value > threshold) {
            alerts.push({
                parameter: param.label,
                current: param.value.toFixed(2),
                threshold: threshold,
                severity: param.value > threshold * 1.5 ? 'high' : 'medium'
            });
        }
    });
    
    displayAlerts(alerts);
    
    // Update alert count in quick stats
    const alertCount = document.getElementById('alertCount');
    const alertsBadge = document.getElementById('alertsBadge');
    
    if (alertCount) alertCount.textContent = alerts.length;
    if (alertsBadge) alertsBadge.textContent = alerts.length;
}


// Add auto-select and refresh functionality
function initializeDeviceQuickActions() {
    const refreshBtn = document.getElementById('refreshDeviceList');
    const autoSelectBtn = document.getElementById('autoSelectDevice');
    
    if (refreshBtn) {
        refreshBtn.addEventListener('click', function() {
            // Reload the page to refresh device list
            window.location.reload();
        });
    }
    
    if (autoSelectBtn) {
        autoSelectBtn.addEventListener('click', function() {
            const deviceSelect = document.getElementById('deviceSelect');
            if (deviceSelect && deviceSelect.options.length > 1) {
                // Select the first available device
                deviceSelect.selectedIndex = 1;
                deviceSelect.dispatchEvent(new Event('change'));
                createAlert('Auto-selected first available device', 'success');
            } else {
                createAlert('No devices available for auto-selection', 'warning');
            }
        });
    }
}

function displayAlerts(alerts) {
    const alertsContainer = document.getElementById('alertsContainer');
    const thresholdAlertsCard = document.getElementById('thresholdAlerts');
    
    if (!alertsContainer || !thresholdAlertsCard) return;
    
    if (alerts.length === 0) {
        thresholdAlertsCard.style.display = 'none';
        return;
    }
    
    thresholdAlertsCard.style.display = 'block';
    alertsContainer.innerHTML = alerts.map(alert => {
        const badgeClass = alert.severity === 'high' ? 'bg-danger' : 'bg-warning';
        const alertClass = alert.severity === 'high' ? 'danger' : 'warning';
        
        return `
            <div class="alert alert-${alertClass} alert-dismissible fade show mb-2" role="alert">
                <div class="d-flex align-items-center">
                    <i class="bi bi-exclamation-triangle-fill me-2"></i>
                    <div class="flex-grow-1">
                        <strong>${alert.parameter}</strong> exceeds threshold!
                        <br>
                        <small>
                            Current: <span class="badge ${badgeClass}">${alert.current} μg/m³</span>
                            Threshold: <span class="badge bg-secondary">${alert.threshold} μg/m³</span>
                        </small>
                    </div>
                </div>
                <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
            </div>
        `;
    }).join('');
}

function calculateThresholdFrequency(data) {
    if (!data.history || !data.status || !data.status.thresholds || !charts.thresholdFrequencyChart) return;
    
    const thresholds = data.status.thresholds;
    const pmTypes = ['pm1', 'pm2_5', 'pm4', 'pm10', 'tsp'];
    const thresholdKeys = ['pm1', 'pm2.5', 'pm4', 'pm10', 'tsp'];
    
    const frequencies = pmTypes.map((type, index) => {
        const values = data.history[type] || [];
        const threshold = thresholds[thresholdKeys[index]];
        if (!threshold || values.length === 0) return 0;
        
        const exceedCount = values.filter(val => val && val > threshold).length;
        return (exceedCount / values.length) * 100;
    });
    
    charts.thresholdFrequencyChart.data.datasets[0].data = frequencies;
    applyRigidAxisForBar('thresholdFrequencyChart', charts.thresholdFrequencyChart, frequencies, 0, 100);
    safeChartUpdate(charts.thresholdFrequencyChart, 'thresholdFrequencyChart');
}

// ========================
// RELAY CONTROL (Enhanced)
// ========================

function setupRelayControls() {
    const modeRadios = document.querySelectorAll('input[name="relayMode"]');
    const manualControls = document.getElementById('manualControls');
    const autoControls = document.getElementById('autoControls');
    
    if (!modeRadios.length) return;
    
    modeRadios.forEach(radio => {
        radio.addEventListener('change', function() {
            if (this.value === 'manual') {
                if (manualControls) manualControls.style.display = 'block';
                if (autoControls) autoControls.style.display = 'none';
            } else {
                if (manualControls) manualControls.style.display = 'none';
                if (autoControls) autoControls.style.display = 'block';
            }
        });
    });
}

function controlRelay(state) {
    if (!currentDeviceId) {
        createAlert('Please select a device first', 'warning');
        return;
    }
    
    apiFetch('/api/relay_control', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            device_id: currentDeviceId,
            state: state
        })
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            createAlert(`Relay ${state} command sent successfully`, 'success');
            // Update relay status immediately
            const relayState = document.getElementById('relayState');
            if (relayState) {
                relayState.textContent = state;
                relayState.className = `badge ${state === 'ON' ? 'bg-success' : 'bg-secondary'}`;
            }
        } else {
            createAlert(data.message || 'Failed to control relay', 'danger');
        }
    })
    .catch(error => {
        console.error('Error controlling relay:', error);
        createAlert('Failed to send relay command', 'danger');
    });
}

function updateAutoMode() {
    const threshold = document.getElementById('autoThreshold').value;
    
    if (!currentDeviceId) {
        createAlert('Please select a device first', 'warning');
        return;
    }
    
    apiFetch('/api/relay_control', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            device_id: currentDeviceId,
            mode: 'auto',
            auto_threshold: parseFloat(threshold)
        })
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            createAlert('Auto mode threshold updated successfully', 'success');
        } else {
            createAlert(data.message || 'Failed to update auto mode', 'danger');
        }
    })
    .catch(error => {
        console.error('Error updating auto mode:', error);
        createAlert('Failed to update auto mode', 'danger');
    });
}

// ========================
// DEEP ANALYTICS FUNCTIONS (New)
// ========================

function updateParameterTrendChart(parameter) {
    if (!parameter || !charts.paramTrendChart) return;
    
    const chart = charts.paramTrendChart;
    chart.data.datasets[0].label = getParameterLabel(parameter);
    chart.data.datasets[0].borderColor = colorScheme[parameter] || 'rgba(75, 192, 192, 0.8)';
    chart.data.datasets[0].backgroundColor = (colorScheme[parameter] || 'rgba(75, 192, 192, 0.8)').replace('0.8', '0.2');
    chart.update();
}

function updateHistogramChart(parameter) {
    if (!parameter || !charts.histogramChart) return;
    
    const chart = charts.histogramChart;
    chart.data.datasets[0].label = `${getParameterLabel(parameter)} Distribution`;
    chart.data.datasets[0].backgroundColor = colorScheme[parameter] || 'rgba(75, 192, 192, 0.6)';
    chart.update();
}

function updateCorrelationChart() {
    const paramX = document.getElementById('corrParamX').value;
    const paramY = document.getElementById('corrParamY').value;
    
    if (!paramX || !paramY || !charts.correlationScatter) return;
    
    charts.correlationScatter.options.scales.x.title.text = getParameterLabel(paramX);
    charts.correlationScatter.options.scales.y.title.text = getParameterLabel(paramY);
    charts.correlationScatter.update();
}

function getParameterLabel(param) {
    const labels = {
        pm1: 'PM1 (μg/m³)',
        pm2_5: 'PM2.5 (μg/m³)',
        pm4: 'PM4 (μg/m³)',
        pm10: 'PM10 (μg/m³)',
        tsp: 'TSP (μg/m³)',
        temperature_c: 'Temperature (°C)',
        humidity_percent: 'Humidity (%)',
        pressure_hpa: 'Pressure (hPa)',
        voc_ppb: 'VOC (ppb)',
        no2_ppb: 'NO₂ (ppb)',
        gps_speed_kmh: 'Speed (km/h)',
        cloud_cover_percent: 'Cloud Cover (%)',
        voc: 'VOC (ppb)',
        no2: 'NO₂ (ppb)',
        speed: 'Speed (km/h)',
        cloud: 'Cloud Cover (%)'
    };
    return labels[param] || param;
}

// ========================
// CHART CONTROLS (New)
// ========================

function toggleChartType(type) {
    if (charts.environmentalCombinedChart) {
        charts.environmentalCombinedChart.config.type = type;
        charts.environmentalCombinedChart.update();
        createAlert(`Chart type changed to ${type}`, 'info');
    }
}

function toggleUnifiedChartType(type) {
    if (charts.unifiedChart) {
        charts.unifiedChart.config.type = type;
        charts.unifiedChart.update();
        createAlert(`Unified chart type changed to ${type}`, 'info');
    }
}

function updateUnifiedChartStatistics(data) {
    // Update unified chart statistics
    if (!data || !data.history) return;

    const isNumber = (value) => typeof value === 'number' && Number.isFinite(value);
    const firstValid = (arr) => (Array.isArray(arr) ? arr.find(isNumber) : undefined);
    const validSeries = (arr) => (Array.isArray(arr) ? arr.filter(isNumber) : []);
    const display = (value, suffix = '') => isNumber(value) ? `${value.toFixed(1)}${suffix}` : `--${suffix}`;
    
    const pm25Data = validSeries(data.history.pm2_5 || []);
    const no2Data = validSeries(data.history.no2_ppb || []);
    const vocData = validSeries(data.history.voc_ppb || []);
    const noiseData = validSeries(data.history.noise_db || []);

    const latestPm1 = isNumber(data.extended?.pm1) ? data.extended.pm1 : firstValid(data.history.pm1 || []);
    const latestPm25 = isNumber(data.extended?.pm2_5) ? data.extended.pm2_5 : firstValid(pm25Data);
    const latestPm4 = isNumber(data.extended?.pm4) ? data.extended.pm4 : firstValid(data.history.pm4 || []);
    const latestPm10 = isNumber(data.extended?.pm10) ? data.extended.pm10 : firstValid(data.history.pm10 || []);
    const latestTemp = isNumber(data.extended?.temperature_c) ? data.extended.temperature_c : firstValid(data.history.temperature || []);
    const latestHumid = isNumber(data.extended?.humidity_percent) ? data.extended.humidity_percent : firstValid(data.history.humidity || []);
    const latestPressure = isNumber(data.extended?.pressure_hpa) ? data.extended.pressure_hpa : firstValid(data.history.pressure_hpa || []);
    const latestNo2 = isNumber(data.extended?.no2_ppb) ? data.extended.no2_ppb : firstValid(no2Data);
    const latestVoc = isNumber(data.extended?.voc_ppb) ? data.extended.voc_ppb : firstValid(vocData);
    const latestNoise = isNumber(data.extended?.noise_db) ? data.extended.noise_db : firstValid(noiseData);
    
    // Update PM statistics
    const maxPM25 = pm25Data.length > 0 ? Math.max(...pm25Data) : null;
    const avgPM25 = pm25Data.length > 0 ? (pm25Data.reduce((a, b) => a + b, 0) / pm25Data.length) : null;
    
    document.getElementById('unified-pm1').textContent = display(latestPm1, ' μg/m³');
    document.getElementById('unified-pm25').textContent = display(latestPm25, ' μg/m³');
    document.getElementById('unified-pm4').textContent = display(latestPm4, ' μg/m³');
    document.getElementById('unified-pm10').textContent = display(latestPm10, ' μg/m³');
    
    // Update environmental statistics
    document.getElementById('unified-temp').textContent = display(latestTemp, ' °C');
    document.getElementById('unified-humid').textContent = display(latestHumid, ' %');
    document.getElementById('unified-pressure').textContent = display(latestPressure, ' hPa');
    
    // Update air quality statistics
    document.getElementById('unified-no2').textContent = display(latestNo2, ' ppb');
    document.getElementById('unified-voc').textContent = display(latestVoc, ' ppb');
    document.getElementById('unified-noise').textContent = display(latestNoise, ' dB');
    
    // Update aggregate statistics
    document.getElementById('unified-max-pm25').textContent = isNumber(maxPM25) ? `${maxPM25.toFixed(2)} μg/m³` : '-- μg/m³';
    document.getElementById('unified-avg-pm25').textContent = isNumber(avgPM25) ? `${avgPM25.toFixed(2)} μg/m³` : '-- μg/m³';
    
    // Calculate and display AQI (prefer backend value)
    if (data.aqi && data.aqi.current && Number.isFinite(data.aqi.current.index)) {
        document.getElementById('unified-aqi').textContent = String(data.aqi.current.index);
    } else if (isNumber(avgPM25)) {
        const aqi = calculateAQI(avgPM25);
        document.getElementById('unified-aqi').textContent = Number.isFinite(aqi) ? aqi.toFixed(0) : '--';
    } else {
        document.getElementById('unified-aqi').textContent = '--';
    }
}

function calculateAQI(pm25) {
    // CPCB (India) PM2.5 AQI calculation
    if (pm25 === null || pm25 === undefined || pm25 === '') return null;
    const c = Math.round(Number(pm25));
    
    const interpolate = (val, cLow, cHigh, iLow, iHigh) => {
        return Math.round(((iHigh - iLow) / (cHigh - cLow)) * (val - cLow) + iLow);
    };

    if (c <= 30)  return interpolate(c, 0, 30, 0, 50);
    if (c <= 60)  return interpolate(c, 31, 60, 51, 100);
    if (c <= 90)  return interpolate(c, 61, 90, 101, 200);
    if (c <= 120) return interpolate(c, 91, 120, 201, 300);
    if (c <= 250) return interpolate(c, 121, 250, 301, 400);
    if (c <= 380) return interpolate(c, 251, 380, 401, 500);
    return 500;
}

function toggleChartFill() {
    if (charts.timeChart) {
        charts.timeChart.data.datasets.forEach(dataset => {
            dataset.fill = !dataset.fill;
        });
        charts.timeChart.update();
        createAlert('Chart fill toggled', 'info');
    }
}

function resetZoom() {
    Object.values(charts).forEach(chart => {
        if (chart && chart.resetZoom) {
            chart.resetZoom();
        }
    });
    createAlert('Chart zoom reset', 'info');
}

function refreshData() {
    if (currentDeviceId) {
        fetchData(24);
        createAlert('Data refreshed', 'success');
    } else {
        createAlert('Please select a device first', 'warning');
    }
}

// ========================
// REAL-TIME MODE (Enhanced from your existing)
// ========================

// Real-time mode removed: app always runs in real-time via websockets

// ========================
// THRESHOLD SETTINGS (Enhanced)
// ========================

function updateThresholdForm(thresholds) {
    const thresholdSettings = document.getElementById('thresholdSettings');
    if (!thresholdSettings || !thresholds) return;
    
    const params = [
        { key: 'pm1', label: 'PM1' },
        { key: 'pm2.5', label: 'PM2.5' },
        { key: 'pm4', label: 'PM4' },
        { key: 'pm10', label: 'PM10' },
        { key: 'tsp', label: 'TSP' }
    ];
    
    thresholdSettings.innerHTML = params.map(param => `
        <div class="mb-3">
            <label for="threshold_${param.key}" class="form-label">${param.label} Threshold (μg/m³)</label>
            <div class="input-group">
                <input type="number" class="form-control" id="threshold_${param.key}" 
                       value="${thresholds[param.key] || 0}" min="0" step="0.1">
                <span class="input-group-text">μg/m³</span>
            </div>
        </div>
    `).join('') + `
        <div class="d-grid">
            <button class="btn btn-primary" onclick="updateThresholds()">
                <i class="bi bi-check2 me-2"></i>Update Thresholds
            </button>
        </div>
    `;
}

function updateThresholds() {
    if (!currentDeviceId) {
        createAlert('Please select a device first', 'warning');
        return;
    }
    
    const thresholds = {
        pm1: parseFloat(document.getElementById('threshold_pm1').value),
        'pm2.5': parseFloat(document.getElementById('threshold_pm2.5').value),
        pm4: parseFloat(document.getElementById('threshold_pm4').value),
        pm10: parseFloat(document.getElementById('threshold_pm10').value),
        tsp: parseFloat(document.getElementById('threshold_tsp').value)
    };
    
    apiFetch(`/api/update_thresholds?deviceid=${encodeURIComponent(currentDeviceId)}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(thresholds)
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            createAlert('Thresholds updated successfully', 'success');
            fetchData(0.25); // Refresh current data
        } else {
            createAlert(data.message || 'Failed to update thresholds', 'danger');
        }
    })
    .catch(error => {
        console.error('Error updating thresholds:', error);
        createAlert('Failed to update thresholds', 'danger');
    });
}

// ========================
// DATA EXPORT (Enhanced from your existing)
// ========================

// ========================
// MISSING HELPER FUNCTIONS
// ========================
function getParameterLabel(param) {
    const labels = {
        'pm1': 'PM1 (μg/m³)',
        'pm2_5': 'PM2.5 (μg/m³)',
        'pm4': 'PM4 (μg/m³)',
        'pm10': 'PM10 (μg/m³)',
        'tsp': 'TSP (μg/m³)',
        'temperature_c': 'Temperature (°C)',
        'humidity_percent': 'Humidity (%)',
        'pressure_hpa': 'Pressure (hPa)',
        'voc_ppb': 'VOC (ppb)',
        'no2_ppb': 'NO₂ (ppb)',
        'gps_speed_kmh': 'Speed (km/h)',
        'cloud_cover_percent': 'Cloud Cover (%)'
    };
    return labels[param] || param;
}



function updateDeviceLocation(lat, lon) {
    if (map && deviceMarker) {
        deviceMarker.setLatLng([lat, lon]);
        map.setView([lat, lon], map.getZoom());
    }
}

// Add missing functions for other charts
function checkThresholds(data) {
    // Basic threshold checking logic
    if (!data.sensor || !data.status || !data.status.thresholds) return;
    
    const sensor = data.sensor;
    const thresholds = data.status.thresholds;
    
    const exceedances = [];
    if (sensor.pm1 > thresholds.pm1) exceedances.push('PM1');
    if (sensor.pm2_5 > thresholds['pm2.5']) exceedances.push('PM2.5');
    if (sensor.pm4 > thresholds.pm4) exceedances.push('PM4');
    if (sensor.pm10 > thresholds.pm10) exceedances.push('PM10');
    if (sensor.tsp > thresholds.tsp) exceedances.push('TSP');
    
    if (exceedances.length > 0) {
        console.log('Threshold exceedances:', exceedances);
    }
}

function calculateThresholdFrequency(data) {
    // Calculate threshold frequency for display
    if (!data.history || !data.status || !data.status.thresholds) return;
    
    const history = data.history;
    const thresholds = data.status.thresholds;
    const totalReadings = history.timestamps ? history.timestamps.length : 0;
    
    if (totalReadings === 0) return;
    
    const frequencies = [0, 0, 0, 0, 0];
    const params = ['pm1', 'pm2_5', 'pm4', 'pm10', 'tsp'];
    const thresholdKeys = ['pm1', 'pm2.5', 'pm4', 'pm10', 'tsp'];
    
    for (let i = 0; i < totalReadings; i++) {
        params.forEach((param, index) => {
            const value = history[param] && history[param][i];
            const threshold = thresholds[thresholdKeys[index]];
            if (value && threshold && value > threshold) {
                frequencies[index]++;
            }
        });
    }
    
    // Update frequency chart
    if (charts.thresholdFrequencyChart) {
        const percentages = frequencies.map(freq => (freq / totalReadings) * 100);
        charts.thresholdFrequencyChart.data.datasets[0].data = percentages;
        safeChartUpdate(charts.thresholdFrequencyChart, 'thresholdFrequencyChart');
    }
}

function updateQuickStats(data) {
    if (!data || !data.sensor) {
        // Set default values if no data
        const cc = document.getElementById('cloudCover');
        if (cc) cc.textContent = '--%';
        const nl = document.getElementById('noiseLevel');
        if (nl) nl.textContent = '-- dB';
        return;
    }
    
    // Update export count badge
    const exportCount = document.getElementById('exportCount');
    if (exportCount && data && data.total_records !== undefined) {
        exportCount.textContent = `${data.total_records} records`;
    }
    
    // Update cloud cover
    const cloudCover = document.getElementById('cloudCover');
    if (cloudCover && data.extended && data.extended.cloud_cover_percent !== undefined) {
        cloudCover.textContent = `${data.extended.cloud_cover_percent.toFixed(1)}%`;
    } else if (cloudCover) {
        cloudCover.textContent = '--%';
    }
    
    // Update noise level
    const noiseLevel = document.getElementById('noiseLevel');
    if (noiseLevel && data.extended) {
        const noiseVal = data.extended.noise_db !== undefined && data.extended.noise_db !== null ? data.extended.noise_db : data.extended.sound;
        if (typeof noiseVal === 'number') {
            noiseLevel.textContent = `${noiseVal.toFixed(1)} dB`;
        } else {
            noiseLevel.textContent = '-- dB';
        }
    } else if (noiseLevel) {
        noiseLevel.textContent = '-- dB';
    }
}

function normalizeIncomingData(data) {
    if (!data) return null;

    const normalized = JSON.parse(JSON.stringify(data));

    // Map extended data fields - CRITICAL SECTION
    if (data.extended) {
        normalized.extended = {
            temperature_c: data.extended.temperature_c,
            humidity_percent: data.extended.humidity_percent,
            pressure_hpa: data.extended.pressure_hpa,
            voc_ppb: data.extended.voc_ppb,
            no2_ppb: data.extended.no2_ppb,
            cloud_cover_percent: data.extended.cloud_cover_percent,
            lux: data.extended.lux,
            uv_index: data.extended.uv_index,
            battery_percent: data.extended.battery_percent,
            pm2_5: data.extended.pm2_5,
            noise_db: data.extended.noise_db !== undefined ? data.extended.noise_db : data.extended.sound,
            gps_lat: data.extended.gps_lat,
            gps_lon: data.extended.gps_lon,
            gps_alt_m: data.extended.gps_alt_m,
            gps_speed_kmh: data.extended.gps_speed_kmh,
            timestamp: data.extended.timestamp
        };
        console.log('Normalized extended data:', normalized.extended);
    }

    // Ensure sensor data exists
    if (!normalized.sensor && data.sensor) {
        normalized.sensor = data.sensor;
    }

    return normalized;
}




async function exportData() {
    if (!currentDeviceId) {
        createAlert('Please select a device first', 'warning');
        return;
    }
    
    const startDate = document.getElementById('start-date').value;
    const endDate = document.getElementById('end-date').value;
    
    if (!startDate || !endDate) {
        createAlert('Please select both start and end dates', 'warning');
        return;
    }
    
    createAlert('Exporting CSV data, please wait...', 'info');
    
    try {
        const url = `/api/export_csv?deviceid=${currentDeviceId}&start_date=${startDate}&end_date=${endDate}`;
        const response = await apiFetch(url);
        
        if (!response) {
            // apiFetch handles 401 redirect
            return;
        }
        
        if (!response.ok) {
            let errorMessage = 'Failed to export data';
            try {
                const text = await response.text();
                const match = text.match(/<p>(.*?)<\/p>/);
                if (match && match[1]) {
                    errorMessage = match[1].replace(/Error:\s*/, '');
                } else {
                    errorMessage = `Server returned status ${response.status}`;
                }
            } catch (e) {
                errorMessage = `Server returned status ${response.status}`;
            }
            createAlert(errorMessage, 'danger');
            return;
        }
        
        // Get filename from Content-Disposition header if available
        let filename = `dust_data_${currentDeviceId}_${startDate}_to_${endDate}.csv`;
        const disposition = response.headers.get('Content-Disposition');
        if (disposition && disposition.indexOf('attachment') !== -1) {
            const filenameRegex = /filename[^;=\n]*=((['"]).*?\2|[^;\n]*)/;
            const matches = filenameRegex.exec(disposition);
            if (matches != null && matches[1]) { 
                filename = matches[1].replace(/['"]/g, '');
            }
        }
        
        const blob = await response.blob();
        const downloadUrl = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = downloadUrl;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        window.URL.revokeObjectURL(downloadUrl);
        
        createAlert('CSV Export completed successfully', 'success');
    } catch (error) {
        console.error('Export error:', error);
        createAlert('An error occurred during export', 'danger');
    }
}

function setDefaultDates() {
    const today = new Date().toISOString().split('T')[0];
    const weekAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
    
    const startDate = document.getElementById('start-date');
    const endDate = document.getElementById('end-date');
    
    if (startDate) startDate.value = weekAgo;
    if (endDate) endDate.value = today;
}

// ========================
// UTILITY FUNCTIONS (Enhanced from your existing)
// ========================

function updateActiveTimeButton(hours) {
    document.querySelectorAll('.time-range-buttons .btn').forEach(btn => {
        btn.classList.remove('active');
    });
    
    const buttonText = hours === 0.25 ? '15m' : hours === 3 ? '3h' : hours === 6 ? '6h' : '24h';
    const activeButton = Array.from(document.querySelectorAll('.time-range-buttons .btn'))
        .find(btn => btn.getAttribute('data-time') === buttonText);
    
    if (activeButton) {
        activeButton.classList.add('active');
    }
}

// Show loading indicator (your existing enhanced)
function showLoading() {
    const loadingIndicator = document.createElement('div');
    loadingIndicator.className = 'loading-indicator';
    loadingIndicator.innerHTML = `
        <div class="text-center">
            <div class="spinner-border text-primary" role="status" style="width: 3rem; height: 3rem;">
                <span class="visually-hidden">Loading...</span>
            </div>
            <div class="mt-3">
                <h5>Loading data...</h5>
                <div class="progress" style="width: 200px;">
                    <div class="progress-bar progress-bar-striped progress-bar-animated" 
                         style="width: 100%"></div>
                </div>
            </div>
        </div>
    `;
    
    // Center the loading indicator
    loadingIndicator.style.cssText = `
        position: fixed;
        top: 0;
        left: 0;
        width: 100%;
        height: 100%;
        background-color: rgba(0,0,0,0.5);
        display: flex;
        justify-content: center;
        align-items: center;
        z-index: 9999;
        backdrop-filter: blur(2px);
    `;
    
    document.body.appendChild(loadingIndicator);
    return loadingIndicator;
}

// Enhanced alert system with toast notifications
function createAlert(message, type = 'info') {
    // Create Bootstrap toast
    const toastContainer = document.querySelector('.toast-container') || createToastContainer();
    
    const toastId = 'toast-' + Date.now();
    const iconMap = {
        success: 'bi-check-circle',
        danger: 'bi-x-circle',
        warning: 'bi-exclamation-triangle',
        info: 'bi-info-circle'
    };
    
    const toast = document.createElement('div');
    toast.className = 'toast';
    toast.id = toastId;
    toast.setAttribute('role', 'alert');
    toast.innerHTML = `
        <div class="toast-header">
            <i class="bi ${iconMap[type]} me-2 text-${type}"></i>
            <strong class="me-auto">Environmental Monitor</strong>
            <small>${new Date().toLocaleTimeString()}</small>
            <button type="button" class="btn-close" data-bs-dismiss="toast"></button>
        </div>
        <div class="toast-body">
            ${message}
        </div>
    `;
    
    toastContainer.appendChild(toast);
    
    // Initialize and show toast
    const bsToast = new bootstrap.Toast(toast, { delay: 5000 });
    bsToast.show();
    
    // Remove toast element after it's hidden
    toast.addEventListener('hidden.bs.toast', function() {
        toast.remove();
    });
}

function createToastContainer() {
    const container = document.createElement('div');
    container.className = 'toast-container position-fixed top-0 end-0 p-3';
    container.style.zIndex = '11';
    document.body.appendChild(container);
    return container;
}

// ========================
// INITIALIZATION CALLS
// ========================

// Initialize charts when DOM is loaded (your existing)
document.addEventListener('DOMContentLoaded', function() {
    // Charts are already initialized in the main DOMContentLoaded listener above
});

// For backward compatibility with your existing code
window.timeChart = charts.timeChart;
window.thresholdChart = charts.thresholdChart;
window.thresholdFrequencyChart = charts.thresholdFrequencyChart;

// Debug function to test device selection and tab switching
window.testDeviceSelection = function() {
    console.log('=== TESTING DEVICE SELECTION ===');

    const deviceSelect = document.getElementById('deviceSelect');
    console.log('Device select element:', deviceSelect);
    console.log('Device select options:', deviceSelect ? deviceSelect.options.length : 'N/A');
    console.log('Currently selected index:', deviceSelect ? deviceSelect.selectedIndex : 'N/A');
    console.log('Currently selected value:', deviceSelect ? deviceSelect.value : 'N/A');

    if (deviceSelect && deviceSelect.options.length > 1) {
        console.log('=== TESTING MANUAL DEVICE SELECTION ===');
        // Select the first device (skip the "Select a device..." option)
        deviceSelect.selectedIndex = 1;
        console.log('Manually selected device index:', deviceSelect.selectedIndex);
        console.log('Selected device value:', deviceSelect.value);

        // Get the selected option attributes
        const selectedOption = deviceSelect.options[deviceSelect.selectedIndex];
        console.log('Selected option data-type:', selectedOption.getAttribute('data-type'));
        console.log('Selected option data-relay:', selectedOption.getAttribute('data-relay'));

        // Trigger the change event
        console.log('Triggering change event...');
        deviceSelect.dispatchEvent(new Event('change'));
    } else {
        console.log('No devices available for selection');
    }

    console.log('=== DEVICE SELECTION TEST COMPLETE ===');
};

// Debug function to test extended data display
window.testExtendedData = function() {
    console.log('=== TESTING ENVIRONMENTAL CARD ELEMENTS ===');

    // Test if environmental cards exist
    const cardIds = ['currentTemp', 'currentHumidity', 'currentPressure', 'currentVOC', 'currentLux', 'currentUV'];
    const results = {};

    cardIds.forEach(id => {
        const element = document.getElementById(id);
        results[id] = {
            exists: !!element,
            visible: element ? element.offsetParent !== null : false,
            currentText: element ? element.textContent : 'N/A'
        };
        console.log(`${id}:`, results[id]);
    });

    // Test direct update
    console.log('=== TESTING DIRECT CARD UPDATES ===');
    const testData = {
        temperature_c: 24.5,
        humidity_percent: 65.2,
        pressure_hpa: 1013.2,
        voc_ppb: 150,
        lux: 350,
        uv_index: 3.5
    };

    // Test updateExtendedData function directly
    console.log('=== TESTING updateExtendedData FUNCTION ===');
    updateExtendedData(testData);

    console.log('=== TEST COMPLETE ===');
    createAlert('Environmental card test completed - check console', 'info');
};

// Debug function to test API data flow
window.testAPIData = function() {
    console.log('=== TESTING API DATA FLOW ===');

    if (!currentDeviceId) {
        console.log('❌ No device selected');
        return;
    }

    console.log('📡 Fetching data from API...');
    apiFetch(`/api/data?hours=1&deviceid=${currentDeviceId}`)
        .then(response => response.json())
        .then(data => {
            console.log('📊 API Response:', data);
            console.log('🔍 Extended data in response:', data.extended);

            if (data.extended) {
                console.log('✅ Calling updateExtendedData with API data...');
                updateExtendedData(data.extended);
            } else {
                console.log('❌ No extended data in API response');
            }
        })
        .catch(error => {
            console.error('❌ API fetch error:', error);
        });
};

// Debug function to test chart updates
window.testChartUpdates = function() {
    console.log('=== TESTING CHART UPDATES ===');

    // Check if charts exist
    console.log('tempHumidityChart exists:', !!charts.tempHumidityChart);
    console.log('pressureAirQualityChart exists:', !!charts.pressureAirQualityChart);

    // Create test data
    const testData = {
        extended: {
            temperature_c: 25.0,
            humidity_percent: 60.0,
            pressure_hpa: 1015.0,
            voc_ppb: 150
        },
        history: {
            extended: {
                timestamps: [new Date(), new Date(Date.now() - 3600000)],
                temperature_c: [25.0, 24.0],
                humidity_percent: [60.0, 65.0],
                pressure_hpa: [1015.0, 1010.0],
                voc_ppb: [150, 140]
            }
        }
    };

    console.log('Calling updateExtendedCharts with test data...');
    updateExtendedCharts(testData);

    console.log('=== CHART UPDATE TEST COMPLETE ===');
    createAlert('Chart update test completed - check console', 'info');
};

// Debug function to test data fetching
window.testDataFetching = function() {
    console.log('=== TESTING DATA FETCHING ===');

    // Check current device
    console.log('Current device ID:', currentDeviceId);
    console.log('Current device type:', currentDeviceType);

    if (!currentDeviceId) {
        console.log('No device selected - cannot test data fetching');
        return;
    }

    // Manually fetch data
    console.log('Manually calling fetchData(24)...');
    fetchData(24);

    console.log('=== DATA FETCHING TEST COMPLETE ===');
};

// Debug function to test WebSocket
window.testWebSocket = function() {
    console.log('=== TESTING WEBSOCKET ===');

    console.log('Socket exists:', !!socket);
    console.log('Socket connected:', socket ? socket.connected : false);
    console.log('Current device room:', currentDeviceRoom);

    if (socket && socket.connected) {
        console.log('WebSocket is connected');
    } else {
        console.log('WebSocket is NOT connected');
    }

    console.log('=== WEBSOCKET TEST COMPLETE ===');
};
