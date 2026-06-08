# Ticket Auto-Assignment System (`TicketMgr`)

A robust, multi-threaded backend service designed for multi-tenant Customer Relationship Management (CRM) environments. This service automates the assignment of incoming customer support tickets to available agents based on configurable department policies (e.g., Round Robin, Least Tickets Remaining) and sends HTTP notifications to external APIs on successful assignment.

---

## 🏗️ System Architecture

The service leverages a multi-level thread hierarchy to scale dynamically across multiple clients and departments.

```mermaid
graph TD
    Main[Main Thread] -->|Polls Central DB | ClientMgr[Client Manager Thread<br>Client_ID]
    ClientMgr -->|Spawns department threads| DeptMgr[Department Manager Thread<br>Client_Dept]
    DeptMgr -->|Assigns tickets based on policy| Agent[Active Agent Selection<br>Round Robin / LTR]
```

### Thread Hierarchy & Lifecycle
1. **Main Thread**:
   * Periodically polls the central database to fetch verified, active tenants.
   * Spawns a **Client Manager Thread** for each newly activated client.
   * Stops threads for clients that are deactivated.
2. **Client Manager Thread** (one per active client):
   * Monitors department configurations for the assigned client.
   * Spawns a **Department Manager Thread** for new departments.
   * Manages thread lifecycle using shared state and termination signals.
3. **Department Manager Thread** (one per department):
   * Periodically queries the database for available agents (`UsersQue` table) and unassigned tickets (`unprocessed_tickets` table).
   * Automatically routes tickets to agents based on the specified assignment policy.

---

## 🔄 Ticket Assignment Flow

```mermaid
sequenceDiagram
    participant DeptMgr as Department Manager Thread
    participant DB as Client Database
    participant API as External CRM API
    
    DeptMgr->>DB: Fetch active agent queues (UsersQue)
    DeptMgr->>DB: Fetch unprocessed tickets (unprocessed_tickets)
    Note over DeptMgr: Apply Assignment Policy (Round Robin / LTR)
    DeptMgr->>DB: UPDATE ticket_details (Assign to Agent)
    DeptMgr->>DB: UPDATE UsersQue (Increment queue count & timestamp)
    DeptMgr->>DB: DELETE from unprocessed_tickets
    DeptMgr->>API: HTTP GET Notification (api URL)
```

---

## 🗄️ Database Schemas

The application accesses a **Central Database** for client registration, and separate **Client Databases** for tenant data isolation.

### 1. Central Database (`czcrm_generic`)
* **`clientRegistrationBasic`**:
  * `registration_id`: Unique identifier for each client.
  * `status`: Set to `1` for active clients.
  * `verificationStatus`: Set to `'VERIFIED'` for verified clients.
  * `auto_assign_flag`: Set to `1` to enable ticket auto-assignment.

### 2. Client Database (`crm_manager_<client_id>`)
* **`departments`**:
  * `dept_id`: Unique department identifier.
  * `dept_email`: Email associated with the department.
  * `dept_phone`: Phone number associated with the department.
  * `policy`: Assignment rules (`'round robin'` or `'least tickets remaining'`).
* **`UsersQue`**: Tracks current ticket loads and timestamps for agents.
  * `user_id`: Agent ID.
  * `dept_id`: Agent's department ID.
  * `ticket_queue_count`: Total active/remaining tickets.
  * `ticket_max_assign_count`: Cumulative assigned tickets today.
  * `ticket_timestamp`: UNIX timestamp of the last assigned ticket.
* **`unprocessed_tickets`**: FIFO queue for tickets awaiting routing.
  * `ticket_id`: Unique ticket identifier.
  * `source`: Ticket source channel (must be configured in `source_arr`).
* **`ticket_details`**: Base ticket record updated upon assignment.
  * `ticket_id`: Unique ticket identifier.
  * `assigned_to_user_id`: Agent ID assigned.
  * `assigned_to_dept_id`: Department ID assigned.

---

## ⚙️ Configuration File (`autoTicket.conf`)

Stored under `/etc/logConfig/autoTicket.conf`. This configuration dictates the database servers, API targets, and source channels:

```ini
[Mysql_Info]
api=http://czqacrm.c-zentrix.com/CZCRM/auto_assignment_api.php?
source_arr=call,email,chat,twitter,ivr,manual,whatsapp,bulk upload,web
centralDb=czcrm_generic
clientDb=crm_manager

[mysql_primary_write]
mysqlhost=172.16.3.223
mysqluser=root
mysqlpasswd=czentrix

[mysql_secodary_write]
mysqlhost=10.30.1.28
mysqluser=ticketing
mysqlpasswd=czentrix

[mysql_primary_read]
mysqlhost=172.16.3.223
mysqluser=root
mysqlpasswd=czentrix

[mysql_secodary_read]
mysqlhost=10.30.1.28
mysqluser=ticketing
mysqlpasswd=czentrix
```

---

## 🚀 Installation & Deployment

### Prerequisites
* Python `3.6.7`
* MySQL Database access (Primary and Secondary/Failover replica nodes)

### Step-by-Step Initial Setup
Here is the step-by-step flow to run the application for the first time after pulling the code from Git:

1. **Pull or Clone the Repository**:
   Clone the repository or pull the latest changes directly into the designated project directory path `/Czentrix/apps/TicketMgr`.
   > [!IMPORTANT]
   > The `create_env.sh` script is hardcoded to use `/Czentrix/apps/TicketMgr`. Ensure the directory name and path match exactly.
   ```bash
   # Clone directly to the target path
   git clone <repository_url> /Czentrix/apps/TicketMgr

   # Or go to the directory and pull latest
   cd /Czentrix/apps/TicketMgr
   git pull
   ```

2. **Configure Credentials (Pre-setup)**:
   Before executing the setup script, edit `./logConfig/autoTicket.conf` to update your database host, user, password, and CRM API endpoint:
   ```bash
   vi ./logConfig/autoTicket.conf
   ```

3. **Execute the Setup Script**:
   Grant execution rights to `create_env.sh` and run it with root privileges (`sudo`):
   ```bash
   chmod +x create_env.sh
   sudo ./create_env.sh
   ```
   *This will create the virtual environment (`venv`), install package dependencies (using internet or offline wheels), move config files to `/etc/logConfig/`, install systemd services to `/etc/systemd/system/`, and enable the service units.*

4. **Verify Configuration & Log Location**:
   Ensure the configuration is active in the destination folder:
   ```bash
   cat /etc/logConfig/autoTicket.conf
   ```

5. **Start and Manage the Services**:
   Start the service corresponding to the database port you want to process (e.g. 3306):
   ```bash
   sudo systemctl start tt-dialer3306.service
   sudo systemctl status tt-dialer3306.service
   ```

### Offline / Air-Gapped Package Installation
If the deployment server does not have internet access, install the dependencies using the offline Python wheels included in the `PACKAGES` folder:
```bash
pip install -r requirements.txt --no-index --find-links file:///Czentrix/apps/TicketMgr/PACKAGES
```

---

## 🛠️ Service Management

The system operates multiple listener service instances, each bound to a specific database port and socket path.

| Service Name | Port | Unix Socket Path |
| --- | --- | --- |
| `tt-dialer3306.service` | `3306` | `/var/lib/mysql/mysql.sock` |
| `tt-dialer3307.service` | `3307` | `/var/lib/mysql3307/mysql3307.sock` |
| `tt-dialer3308.service` | `3308` | `/var/lib/mysql3308/mysql3308.sock` |
| `tt-dialer3309.service` | `3309` | `/var/lib/mysql3309/mysql3309.sock` |

### Service Control Commands
Use the following commands to manage the service instances:

```bash
# Start a service instance
systemctl start tt-dialer3306.service

# Stop a service instance
systemctl stop tt-dialer3306.service

# Check service status
systemctl status tt-dialer3306.service

# View real-time logs
tail -f /var/log/ticketAssign.log
```

---

## 📜 Coding and Safety Practices
* **Database Failover**: The service handles connection loss gracefully by automatically attempting reconnects (`conn.ping(reconnect=True)`) and switching between primary and secondary database configurations.
* **SQL Injection Prevention**: All queries are fully parameterized using placeholders (`%s`) instead of string formatting.
* **Thread Safety**: Access to shared variables (like department queues and active threads) is protected using `threading.Lock()` to prevent race conditions.
