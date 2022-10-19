# Architecture overview
## Database Schema
The database schema is formed of a set of "tiered" tables.

### Tier 0
**RawData**
: Used to store uploads in raw form, as submitted to the backend for later
asynchronous processing.

### Tier 1
**Data**
: Stores key-value pairs, as extracted from `RawData`

**Upload**
: Each entry defines an upload submitted by a server. An `Upload` has many
`Data` points linked to it and one Server.

**Server**
: Each entry represents a unique server. A Server has many Uploads linked to it.

### Tier 2
**ComputedServerFacts**
: Stores server-related information. These are extracted facts that do not
change over time for a server, such as its `Country`,  or its unique identifier
(`UID`).

**ComputedUploadFacts**
: Stores upload-related information. These are extracted facts that can change
from upload-to-upload. For example `Uptime`.

### Tier 3
**Charts**
: This table stores numerical values in a useful form to be presented by a front
end. This is what is used to offer quick replies to all REST API endpoints.

# Updating requirements.txt
Use pipreqs to generate an up-to-dte requirements.txt
