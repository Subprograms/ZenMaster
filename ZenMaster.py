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
sNextPageUrl    = f"{sZendeskBaseUrl}/api/v2/tickets.json?page[size]=100"
tBasicAuth      = (f"{sAgentEmail}/token", sApiToken)

# Ticket pages traversal in Zendesk
aTicketList = []
while sNextPageUrl:
    oPageResponse = requests.get(sNextPageUrl, auth=tBasicAuth)
    oPageResponse.raise_for_status() # quit upon HTTP error
    dPagePayload = oPageResponse.json()
    aTicketList.extend(dPagePayload.get("tickets", [])) # stash the page
    sNextPageUrl = dPagePayload.get("links", {}).get("next") # next page or None from Zendesk

def cellValue(vRaw):
    if vRaw is None:
        return ""
    if isinstance(vRaw, (dict, list)):
        return json.dumps(vRaw, ensure_ascii=False) # JSON stays JSON, if any
    if isinstance(vRaw, str):
        return vRaw.replace("\r", " ").replace("\n", " ") # kill line-feeds for tidy display
    return vRaw # numbers, bools untouched

# work out every column we’ve seen so no field gets lost
aColumnNames = sorted({sKey for dTicket in aTicketList for sKey in dTicket.keys()})
sFileStamp   = datetime.datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S") # timestamped filenames

# CSV for ingestion by Power BI
sCsvFileName = f"zendesk_tickets_{sFileStamp}.csv"
with open(sCsvFileName, "w", newline="", encoding="utf-8-sig") as hCsv:
    oCsvWriter = csv.DictWriter(
        hCsv,
        fieldnames=aColumnNames,
        extrasaction="ignore",
        quoting=csv.QUOTE_ALL,
        lineterminator="\r\n"
    )
    oCsvWriter.writeheader()
    for dTicket in aTicketList:
        oCsvWriter.writerow({sCol: cellValue(dTicket.get(sCol)) for sCol in aColumnNames})

# Optional XLSX for review, formatted as opposed to the raw CSV
bMakeWorkbook = input("Save formatted Excel workbook? (y/n): ").strip().lower() == "y"
sWorkbookName = None
if bMakeWorkbook:
    try:
        import xlsxwriter
    except ImportError:
        print("xlsxwriter not installed, skipping workbook generation.")
    else:
        sWorkbookName = f"zendesk_tickets_{sFileStamp}_formatted.xlsx"
        oWb = xlsxwriter.Workbook(sWorkbookName, {"constant_memory": True})
        oWs = oWb.add_worksheet("tickets")

        oFmtSection = oWb.add_format({"bold": True, "align": "center", "valign": "vcenter",
                                      "bg_color": "#BDD7EE"})
        oFmtHead = oWb.add_format({"bold": True, "border": 1, "text_wrap": True,
                                   "align": "center", "valign": "vcenter",
                                   "bg_color": "#D9E1F2"})
        oFmtField = oWb.add_format({"border": 1, "text_wrap": True,
                                    "align": "center", "valign": "vcenter"})
        oFmtValue = oWb.add_format({"border": 1, "text_wrap": True,
                                    "align": "left",   "valign": "vcenter"})

        nRowCursor = 0
        for dTicket in aTicketList:
            nTicketId = dTicket.get("id", "UNKNOWN")

            # Ticket Title
            oWs.merge_range(nRowCursor, 0, nRowCursor, 1,
                            f"Ticket ID {nTicketId}", oFmtSection)
            oWs.set_row(nRowCursor, 20)
            nRowCursor += 1

            # Header rows for Ticket Field and its Value
            oWs.write(nRowCursor, 0, "Ticket Field", oFmtHead)
            oWs.write(nRowCursor, 1, "Value",        oFmtHead)
            oWs.set_row(nRowCursor, 22)
            nRowCursor += 1

            # Details
            for sField in aColumnNames:
                oWs.write(nRowCursor, 0, sField, oFmtField)
                oWs.write(nRowCursor, 1, cellValue(dTicket.get(sField)), oFmtValue)
                oWs.set_row(nRowCursor, 35)
                nRowCursor += 1

            nRowCursor += 3  # Padding between tables for each ticket

        oWs.set_column(0, 0, 30,  oFmtField)
        oWs.set_column(1, 1, 100, oFmtValue)
        oWb.close()

sEnvFileName = f"zendesk_tickets_{sFileStamp}.env"
with open(sEnvFileName, "w", encoding="utf-8") as hEnv:
    for dTicket in aTicketList:
        nTicketId = dTicket.get("id")
        if nTicketId is None:
            continue
        for sField, vVal in dTicket.items():
            sEnvVar = f'TICKET_{nTicketId}_{re.sub(r"[^A-Za-z0-9]", "_", sField).upper()}'
            hEnv.write(f'{sEnvVar}="{cellValue(vVal)}"\n')

print(f"Wrote {len(aTicketList)} tickets -> {sCsvFileName}")
if sWorkbookName:
    print(f"Wrote formatted workbook -> {sWorkbookName}")
print(f"Wrote ticket-variable file -> {sEnvFileName}")
