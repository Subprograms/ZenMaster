from dotenv import load_dotenv
import os, sys, requests, csv, datetime, json, re

# Loop prompt for a path to the env file for API calls
while True:
    sCredsPath = input("Path to credentials .env (type 'exit' to quit): ").strip()
    if sCredsPath.lower() == "exit":
        sys.exit(0) # I change my mind... exit
    if not os.path.isfile(sCredsPath):
        print("File not found, please ensure a valid path...") # Invalid path
        continue

    load_dotenv(dotenv_path=sCredsPath, override=True) # Loading the env file for API calls
    sZendeskSubdomain = os.getenv("ZENDESK_SUBDOMAIN") # Your Subdomain
    sAgentEmail       = os.getenv("ZENDESK_EMAIL") # Your Email
    sApiToken         = os.getenv("ZENDESK_API_TOKEN") # Your API Token
    if not all([sZendeskSubdomain, sAgentEmail, sApiToken]):
        print("Incomplete .env file...") # Incomplete details
        continue
    break # Credentials seem proper

# Build the base URL and authenticate once, used upon every call
sZendeskBaseUrl = f"https://{sZendeskSubdomain}.zendesk.com"
tBasicAuth      = (f"{sAgentEmail}/token", sApiToken)

dMe   = requests.get(f"{sZendeskBaseUrl}/api/v2/users/me.json", auth=tBasicAuth).json()
nMyId = dMe["user"]["id"]

# Ticket collectors that tag each ticket with its role (assigned / cc / follower / requester)
def harvestTickets(sRoleLabel, sStartUrl):
    sPage = sStartUrl
    while sPage:
        dJ = requests.get(sPage, auth=tBasicAuth).json()
        for dT in dJ.get("tickets", []):
            dT["_role"] = sRoleLabel # stamp the role (internal only)
            aTicketList.append(dT)
        sPage = dJ.get("links", {}).get("next")

def harvestSearch(sRoleLabel, sQuery):
    sPage = f"{sZendeskBaseUrl}/api/v2/search.json?query={sQuery}&per_page=100"
    while sPage:
        dJ = requests.get(sPage, auth=tBasicAuth).json()
        for dHit in dJ.get("results", []):
            if dHit.get("result_type") == "ticket":
                dHit["_role"] = sRoleLabel # stamp the role (internal only)
                aTicketList.append(dHit)
        sPage = dJ.get("next_page")

aTicketList = []
harvestTickets("assigned",  f"{sZendeskBaseUrl}/api/v2/tickets.json?page[size]=100")
harvestSearch("cc",        f"type:ticket+cc:{nMyId}")
harvestSearch("follower",  f"type:ticket+follower:{nMyId}")
harvestSearch("requester", f"type:ticket+requester:{nMyId}")

aTicketList.sort(key=lambda d: d.get("id", 0)) # sort ascending by ID

def cellValue(vRaw):
    if vRaw is None:
        return ""
    if isinstance(vRaw, (dict, list)):
        return json.dumps(vRaw, ensure_ascii=False) # JSON stays JSON
    if isinstance(vRaw, str):
        return vRaw.replace("\r", " ").replace("\n", " ")
    return vRaw # numbers / bools untouched

# work out columns, put id first
aColumnNames = sorted({k for dT in aTicketList for k in dT.keys() if not k.startswith("_")}) # skip _role
if "id" in aColumnNames:
    aColumnNames.remove("id")
aColumnNames.insert(0, "id") # left-most id

sFileStamp = datetime.datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")

# CSV for ingestion by Power BI
sCsvFileName = f"zendesk_tickets_{sFileStamp}.csv"
with open(sCsvFileName, "w", newline="", encoding="utf-8-sig") as hCsv:
    oCsvWriter = csv.DictWriter(
        hCsv,
        fieldnames=aColumnNames,
        extrasaction="ignore",
        quoting=csv.QUOTE_ALL,
        lineterminator="\r\n",
    )
    oCsvWriter.writeheader()
    for dT in aTicketList:
        oCsvWriter.writerow({k: cellValue(dT.get(k)) for k in aColumnNames})

# Optional XLSX for formatted tickets as tables
bMakeWorkbook = input("Save formatted Excel workbook? (y/n): ").strip().lower() == "y"
sWorkbookName = None
if bMakeWorkbook:
    try:
        import xlsxwriter
    except ImportError:
        print("xlsxwriter not installed, skipping workbook.")
    else:
        sWorkbookName = f"zendesk_tickets_{sFileStamp}_formatted.xlsx"
        oWb = xlsxwriter.Workbook(sWorkbookName, {"constant_memory": True})
        oWs = oWb.add_worksheet("tickets")

        oFmtSection = oWb.add_format({"bold": True, "align": "center", "valign": "vcenter", "bg_color": "#BDD7EE"})
        oFmtHead   = oWb.add_format({"bold": True, "border": 1, "text_wrap": True, "align": "center", "valign": "vcenter", "bg_color": "#D9E1F2"})
        oFmtField  = oWb.add_format({"border": 1, "text_wrap": True, "align": "center", "valign": "vcenter"})
        oFmtValue  = oWb.add_format({"border": 1, "text_wrap": True, "align": "left", "valign": "vcenter"})

        nRow = 0
        for dT in aTicketList:
            nTicketId = dT.get("id", "UNKNOWN")
            sRole     = dT.get("_role", "unknown").upper()
            oWs.merge_range(nRow, 0, nRow, 1, f"{sRole} – Ticket {nTicketId}", oFmtSection)
            oWs.set_row(nRow, 20)
            nRow += 1

            oWs.write(nRow, 0, "Ticket Field", oFmtHead)
            oWs.write(nRow, 1, "Value",        oFmtHead)
            oWs.set_row(nRow, 22)
            nRow += 1

            for k in aColumnNames:
                oWs.write(nRow, 0, k,               oFmtField)
                oWs.write(nRow, 1, cellValue(dT.get(k)), oFmtValue)
                oWs.set_row(nRow, 35)
                nRow += 1

            nRow += 3 # gap before next ticket

        oWs.set_column(0, 0, 30,  oFmtField)
        oWs.set_column(1, 1, 100, oFmtValue)
        oWb.close()

# dump per-ticket variables
sEnvFileName = f"zendesk_tickets_{sFileStamp}.env"
with open(sEnvFileName, "w", encoding="utf-8") as hEnv:
    for dT in aTicketList:
        nId = dT.get("id")
        if nId is None:
            continue
        for k, v in dT.items():
            if k.startswith("_"): # skip internal _role
                continue
            sEnvVar = f'TICKET_{nId}_{re.sub(r"[^A-Za-z0-9]", "_", k).upper()}'
            hEnv.write(f'{sEnvVar}="{cellValue(v)}"\n')

print(f"Wrote {len(aTicketList)} tickets -> {sCsvFileName}")
if bMakeWorkbook:
    print(f"Wrote formatted workbook -> {sWorkbookName}")
print(f"Wrote ticket-variable file -> {sEnvFileName}")
