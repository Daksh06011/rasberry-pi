# 🍓 Raspberry Pi (Linux) Deployment & NAS Storage Guide

This guide describes how to run the PM Monitoring Dashboard on your Raspberry Pi (username: `raspberry`) under `/home/raspberry/SGN` and store the SQLite database on a Network Attached Storage (NAS) share with a 3-month (90-day) data retention policy.

---

## 1. Prerequisites & System Packages

Update your package index and install the required dependencies on your Raspberry Pi:

```bash
# Update package list
sudo apt update

# Install Python 3, virtual environment, compile tools, and Nginx
sudo apt install -y python3 python3-pip python3-venv python3-dev build-essential libffi-dev libssl-dev nginx git
```

---

## 2. Mounting Your NAS Storage Persistently

We want to mount your NAS directory so the Raspberry Pi views it as a local folder. First, create a mount point on your Pi:

```bash
# Create mount directory
sudo mkdir -p /mnt/nas

# Grant ownership to your raspberry user
sudo chown -R raspberry:raspberry /mnt/nas
```

Choose **Option A** (NFS) or **Option B** (SMB/Samba) depending on how your NAS share is configured.

### Option A: Mount NFS Share (Recommended for Linux)
1. Open `/etc/fstab` with sudo:
   ```bash
   sudo nano /etc/fstab
   ```
2. Append the following line (replace `192.168.1.100` and `/volume1/nas_share` with your NAS IP address and NFS share path):
   ```text
   192.168.1.100:/volume1/nas_share /mnt/nas nfs defaults,noatime,nofail,x-systemd.automount 0 0
   ```
3. Save and close (`Ctrl+O` then `Enter`, then `Ctrl+X`).

### Option B: Mount SMB/Samba Share (Common for Synology/Windows/TrueNAS)
1. Create a credentials file to securely store your NAS username and password:
   ```bash
   nano ~/.nascredentials
   ```
2. Write your NAS credentials:
   ```text
   username=your_nas_username
   password=your_nas_password
   ```
3. Secure the credentials file:
   ```bash
   chmod 600 ~/.nascredentials
   ```
4. Open `/etc/fstab`:
   ```bash
   sudo nano /etc/fstab
   ```
5. Append the following line (replace `//192.168.1.100/nas_share` with your NAS SMB network path):
   ```text
   //192.168.1.100/nas_share /mnt/nas cifs credentials=/home/raspberry/.nascredentials,uid=raspberry,gid=raspberry,iocharset=utf8,nofail,x-systemd.automount 0 0
   ```
6. Save and close.

### Mount the Share
Execute the mount command to mount the network folder:
```bash
sudo mount -a
```
Confirm the mount is active and writable:
```bash
touch /mnt/nas/test_write.txt && rm /mnt/nas/test_write.txt
```

---

## 3. Environment & Application Setup

1. Navigate to your project folder:
   ```bash
   cd /home/raspberry/SGN
   ```
2. Create/update your `.env` file in the root directory:
   ```bash
   nano .env
   ```
3. Add the following contents (configure host, ports, and point `SQLITE_DB_PATH` to the mounted NAS share folder):
   ```env
   # Flask configuration
   FLASK_SECRET_KEY="generate_a_random_long_secret_key_string"
   FLASK_HOST="0.0.0.0"
   FLASK_PORT="5000"
   FLASK_DEBUG="false"

   # Database Storage Settings
   USE_SQLITE="true"
   SQLITE_DB_PATH="/mnt/nas/pm_monitoring.db"

   # Data Retention (in days) - keeps exactly 3 months of data
   RETENTION_DAYS="90"

   # Default Device ID Fallback
   # When MQTT payloads don't contain deviceid/i/mac (like the temperature/humidity/moisture sensors)
   # they will fall back to this device ID to avoid being ignored.
   DEFAULT_DEVICE_ID="SGN-V3-12"

   # MQTT Configuration
   MQTT_BROKER="broker.hivemq.com"
   MQTT_USERNAME="your_mqtt_username"
   MQTT_PASSWORD="your_mqtt_password"
   ```

---

## 4. Run as a System Daemon (Systemd Service)

To make sure the application launches automatically on boot, runs in the background, and restarts if it fails, set up a Systemd service:

1. Create a service file:
   ```bash
   sudo nano /etc/systemd/system/pm-monitoring.service
   ```
2. Paste the following configuration:
   ```ini
   [Unit]
   Description=PM Monitoring Dashboard
   After=network.target network-online.target mnt-nas.mount
   Wants=network-online.target

   [Service]
   User=raspberry
   WorkingDirectory=/home/raspberry/SGN
   ExecStart=/home/raspberry/SGN/venv/bin/gunicorn -k eventlet -w 1 app:app --bind 0.0.0.0:5000
   Restart=always
   RestartSec=5

   [Install]
   WantedBy=multi-user.target
   ```
3. Save and close the file.
4. Reload systemd, enable the service to start on boot, and start the app:
   ```bash
   sudo systemctl daemon-reload
   sudo systemctl enable pm-monitoring
   sudo systemctl start pm-monitoring
   ```
5. Monitor status and logs to verify startup:
   ```bash
   # Check service status
   sudo systemctl status pm-monitoring

   # Tail logs live
   sudo journalctl -u pm-monitoring -f
   ```

---

## 5. Nginx Reverse Proxy Configuration (Port 80/HTTPS)

To expose the dashboard on port 80 and ensure WebSocket (Socket.IO) connections update in real-time, configure Nginx as a reverse proxy:

1. Remove the default Nginx page:
   ```bash
   sudo rm /etc/nginx/sites-enabled/default
   ```
2. Create a configuration for your dashboard:
   ```bash
   sudo nano /etc/nginx/sites-available/pm-monitoring
   ```
3. Paste the following Nginx block:
   ```nginx
   server {
       listen 80;
       server_name _; # Responds to all requests on your local network (e.g., http://192.168.31.195)

       location / {
           proxy_pass http://127.0.0.1:5000;
           proxy_http_version 1.1;
           
           # WebSocket headers (CRITICAL for real-time dashboard data)
           proxy_set_header Upgrade $http_upgrade;
           proxy_set_header Connection "upgrade";
           
           proxy_set_header Host $host;
           proxy_set_header X-Real-IP $remote_addr;
           proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
           proxy_set_header X-Forwarded-Proto $scheme;
           
           proxy_read_timeout 86400s;
           proxy_send_timeout 86400s;
       }
   }
   ```
4. Enable the configuration and restart Nginx:
   ```bash
   sudo ln -s /etc/nginx/sites-available/pm-monitoring /etc/nginx/sites-enabled/
   sudo nginx -t # Verify configuration syntax
   sudo systemctl restart nginx
   ```

Now open a browser on any device in the local network and visit the IP address of your Raspberry Pi (e.g. `http://192.168.31.195`). You will be greeted by the dashboard!

---

## 6. How the 3-Month Data Retention Works

1. **Automated Thread**: The Flask app spawns a background thread on startup that wakes up every 24 hours.
2. **Purging Execution**: It looks at the `.env` variable `RETENTION_DAYS` (default `90` days).
3. **Database Cleanup**: It runs optimized deletion queries on three tables to prune old data:
   - `dust_sensor_data` (where `timestamp` is older than `90` days)
   - `dust_extended_data` (where `timestamp` is older than `90` days)
   - `dust_device_alerts` (where `created_at` is older than `90` days)
4. **Monitoring**: You can check the service logs using `sudo journalctl -u pm-monitoring` to see when pruning runs and how many records are removed:
   ```text
   [PRUNING] Starting database pruning. Retention window: 90 days...
   [PRUNING] Database pruning complete. Deleted rows: sensor_data=1420, extended_data=1420, alerts=12
   ```

---

## 💡 Reliability & Performance Tips for NAS DB Storage

> [!WARNING]
> SQLite is designed for local disk storage. Storing the DB on a network mount (NFS/CIFS) is fully supported, but network dropouts or high latency can slow down database write operations.
> Follow these steps for optimal network database stability:
>
> 1. **Use Wired Ethernet**: Always connect your Raspberry Pi to the local network using a wired Ethernet connection rather than Wi-Fi.
> 2. **Auto-Mount Security**: The NFS/CIFS mount flags in this guide include `nofail` and `x-systemd.automount` to ensure the Pi boots even if the NAS is temporarily offline, and auto-mounts the share on first access.
> 3. **SQLite WAL Mode**: The dashboard codebase is configured to run in **Write-Ahead Logging (WAL)** mode. WAL mode reduces lock contention and improves read/write speeds over network drives.
