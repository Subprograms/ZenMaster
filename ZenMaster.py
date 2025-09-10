from dotenv import load_dotenv
import os, sys, requests, csv, datetime, json, re

# Loop prompt for a path to the env file for API calls
sScriptDir = os.path.dirname(os.path.abspath(__file__))
sCredsPath = None
for sName in os.listdir(sScriptDir):
    if sName.lower() == "credentials.env":
        sCredsPath = os.path.join(sScriptDir, sName)
        break

if not sCredsPath or not os.path.isfile(sCredsPath):
    print("Missing credentials.env in this folder. Create a file named credentials.env here with the following contents:")
    print("")
    print("ZENDESK_SUBDOMAIN=<Your Subdomain>")
    print("ZENDESK_EMAIL=<Your Email>")
    print("ZENDESK_API_TOKEN=<Your Token>")
    sys.exit(0)

load_dotenv(dotenv_path=sCredsPath, override=True)
sZendeskSubdomain = os.getenv("ZENDESK_SUBDOMAIN")
sAgentEmail       = os.getenv("ZENDESK_EMAIL")
sApiToken         = os.getenv("ZENDESK_API_TOKEN")
if not all([sZendeskSubdomain, sAgentEmail, sApiToken]):
    print("Incomplete .env file...")
    print("")
    print("ZENDESK_SUBDOMAIN=<Your Subdomain>")
    print("ZENDESK_EMAIL=<Your Email>")
    print("ZENDESK_API_TOKEN=<Your Token>")
    sys.exit(0)

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

# -----------------------------
# Filtering Options
# -----------------------------
org_filter = None
time_start_filter = None
time_end_filter = None
date_start_filter = None
date_end_filter = None

recipient_filter = None
requester_id_filter = None
result_type_filter = None
status_filter = None
subject_filter = None
submitter_id_filter = None

# Per-field compiled boolean expressions (list of tuples: (original_str, rpn_tokens))
aOrgExprs = []
aRecipientExprs = []
aRequesterIdExprs = []
aResultTypeExprs = []
aStatusExprs = []
aSubjectExprs = []
aSubmitterIdExprs = []

# Time/date ranges with per-field logic (AND/OR within field)
aTimeRanges = []
aDateRanges = []
sCreatedAtTimeLogic = "OR"
sCreatedAtDateLogic = "OR"

def isValidEmail(sVal):
    if not isinstance(sVal, str):
        return False
    if not (1 <= len(sVal) <= 254):
        return False
    return re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", sVal) is not None

def isValidId(sVal):
    return isinstance(sVal, str) and re.fullmatch(r"\d+", sVal) is not None

def isValidOrgId14(sVal):
    return isinstance(sVal, str) and re.fullmatch(r"\d{14}", sVal) is not None

def isValidStatus(sVal):
    return isinstance(sVal, str) and sVal.lower() in {"new","open","pending","hold","solved","closed"}

def isValidResultType(sVal):
    return isinstance(sVal, str) and sVal.lower() in {"ticket","user","organization","group","comment","article","entry"}

def isValidSubject(sVal):
    return isinstance(sVal, str) and 1 <= len(sVal) <= 200

def chooseListMergeMode(sWhat, nCount):
    while True:
        sMode = input(f"{sWhat} currently has {nCount} value(s). Choose: (a) add, (o) overwrite, (k) keep: ").strip().lower()
        if sMode in ("a","o","k"):
            return sMode
        print("Invalid choice. Enter a, o, or k.")

# -------- Boolean Expression Parsing (AND/OR, parentheses) --------
def tokenizeExpr(sInput):
    aTokens = []
    n = len(sInput)
    i = 0
    while i < n:
        c = sInput[i]
        if c.isspace():
            i += 1
            continue
        if c in "()":
            aTokens.append(c)
            i += 1
            continue
        # Check for AND/OR operators (case-insensitive), otherwise accumulate a value token
        if sInput[i:i+3].upper() == "AND" and (i+3 == n or sInput[i+3].isspace() or sInput[i+3] in "()"):
            aTokens.append("AND")
            i += 3
            continue
        if sInput[i:i+2].upper() == "OR" and (i+2 == n or sInput[i+2].isspace() or sInput[i+2] in "()"):
            aTokens.append("OR")
            i += 2
            continue
        # Value token: read until whitespace or parenthesis
        j = i
        while j < n and not sInput[j].isspace() and sInput[j] not in "()":
            j += 1
        aTokens.append(("VAL", sInput[i:j]))
        i = j
    return aTokens

def validateExprTokens(aTokens, fValidator, sErr):
    for t in aTokens:
        if isinstance(t, tuple) and t[0] == "VAL":
            if not fValidator(t[1]):
                print(sErr)
                return False
    return True

def toRpn(aTokens):
    # Shunting-yard for AND/OR with precedence AND > OR (standard)
    dPrec = {"OR":1, "AND":2}
    aOut = []
    aOps = []
    for t in aTokens:
        if isinstance(t, tuple) and t[0] == "VAL":
            aOut.append(t)
        elif t in ("AND","OR"):
            while aOps and aOps[-1] in ("AND","OR") and dPrec[aOps[-1]] >= dPrec[t]:
                aOut.append(aOps.pop())
            aOps.append(t)
        elif t == "(":
            aOps.append(t)
        elif t == ")":
            bFound = False
            while aOps:
                op = aOps.pop()
                if op == "(":
                    bFound = True
                    break
                aOut.append(op)
            if not bFound:
                raise ValueError("Mismatched parentheses")
        else:
            raise ValueError("Invalid token")
    while aOps:
        op = aOps.pop()
        if op in ("(",")"):
            raise ValueError("Mismatched parentheses")
        aOut.append(op)
    return aOut

def compileExpr(sInput, fValidator, sErr, bLower=False):
    if not isinstance(sInput, str) or not sInput.strip():
        print(sErr)
        return None
    # Normalize whitespace around parentheses
    sNorm = re.sub(r"\s+", " ", sInput.strip())
    try:
        aTokens = tokenizeExpr(sNorm)
        # Lowercase value tokens if requested (for case-insensitive comparisons)
        aTokensNorm = []
        for t in aTokens:
            if isinstance(t, tuple) and t[0] == "VAL":
                v = t[1].lower() if bLower else t[1]
                aTokensNorm.append(("VAL", v))
            else:
                aTokensNorm.append(t)
        if not validateExprTokens(aTokensNorm, fValidator, sErr):
            return None
        aRpn = toRpn(aTokensNorm)
        return (sNorm, aRpn)
    except Exception as e:
        print("Invalid expression. Use values with AND/OR and parentheses.")
        return None

def evalRpn(aRpn, fMatch):
    aStack = []
    for t in aRpn:
        if isinstance(t, tuple) and t[0] == "VAL":
            aStack.append(bool(fMatch(t[1])))
        elif t == "AND":
            if len(aStack) < 2:
                return False
            b2 = aStack.pop(); b1 = aStack.pop()
            aStack.append(b1 and b2)
        elif t == "OR":
            if len(aStack) < 2:
                return False
            b2 = aStack.pop(); b1 = aStack.pop()
            aStack.append(b1 or b2)
        else:
            return False
    return aStack[-1] if aStack else False

# ---------------- Filtering application ----------------
def applyFilters(aTickets):
    def pExprList(aExprs, fMatch):
        # Field-level OR across separate expressions entered across multiple selections
        if not aExprs:
            return True
        for (sExpr, aRpn) in aExprs:
            if evalRpn(aRpn, fMatch):
                return True
        return False

    def pTime(dT):
        if not aTimeRanges:
            return True
        sCreated = dT.get("created_at")
        if not isinstance(sCreated, str) or "T" not in sCreated:
            return False
        sTime = sCreated.split("T")[1]
        aVals = [(sStart <= sTime <= sEnd) for (sStart, sEnd) in aTimeRanges]
        if sCreatedAtTimeLogic == "AND":
            return all(aVals)
        return any(aVals)

    def pDate(dT):
        if not aDateRanges:
            return True
        sCreated = dT.get("created_at")
        if not isinstance(sCreated, str) or "T" not in sCreated:
            return False
        sDate = sCreated.split("T")[0]
        aVals = [(sStart <= sDate <= sEnd) for (sStart, sEnd) in aDateRanges]
        if sCreatedAtDateLogic == "AND":
            return all(aVals)
        return any(aVals)

    aOut = []
    for dT in aTickets:
        # organization_id
        if not pExprList(
            aOrgExprs,
            lambda v: str(dT.get("organization_id")) == v
        ):
            continue
        # created_at_time, created_at_date
        if not pTime(dT):
            continue
        if not pDate(dT):
            continue
        # recipient
        if not pExprList(
            aRecipientExprs,
            lambda v: str(dT.get("recipient") or "").lower() == v
        ):
            continue
        # requester_id
        if not pExprList(
            aRequesterIdExprs,
            lambda v: str(dT.get("requester_id")) == v
        ):
            continue
        # result_type
        if not pExprList(
            aResultTypeExprs,
            lambda v: str(dT.get("result_type") or "").lower() == v
        ):
            continue
        # status
        if not pExprList(
            aStatusExprs,
            lambda v: str(dT.get("status") or "").lower() == v
        ):
            continue
        # subject (contains)
        if not pExprList(
            aSubjectExprs,
            lambda v: v in str(dT.get("subject") or "").lower()
        ):
            continue
        # submitter_id
        if not pExprList(
            aSubmitterIdExprs,
            lambda v: str(dT.get("submitter_id")) == v
        ):
            continue

        aOut.append(dT)
    return aOut

def formatProposition():
    def joinExprs(aExprs):
        if not aExprs:
            return None
        aParts = []
        for (sExpr, _rpn) in aExprs:
            aParts.append("(" + sExpr + ")")
        return " OR ".join(aParts)

    aFieldExprs = []

    s = joinExprs(aOrgExprs)
    if s: aFieldExprs.append(s)

    if aTimeRanges:
        sLogic = sCreatedAtTimeLogic
        aParts = [f'(created_at_time between "{sStart}" and "{sEnd}")' for (sStart, sEnd) in aTimeRanges]
        sField = "(" + f" {sLogic} ".join(aParts) + ")" if len(aParts) > 1 else aParts[0]
        aFieldExprs.append(sField)

    if aDateRanges:
        sLogic = sCreatedAtDateLogic
        aParts = [f'(created_at_date between "{sStart}" and "{sEnd}")' for (sStart, sEnd) in aDateRanges]
        sField = "(" + f" {sLogic} ".join(aParts) + ")" if len(aParts) > 1 else aParts[0]
        aFieldExprs.append(sField)

    s = joinExprs(aRecipientExprs)
    if s: aFieldExprs.append(s)

    s = joinExprs(aRequesterIdExprs)
    if s: aFieldExprs.append(s)

    s = joinExprs(aResultTypeExprs)
    if s: aFieldExprs.append(s)

    s = joinExprs(aStatusExprs)
    if s: aFieldExprs.append(s)

    s = joinExprs(aSubjectExprs)
    if s: aFieldExprs.append(s)

    s = joinExprs(aSubmitterIdExprs)
    if s: aFieldExprs.append(s)

    if not aFieldExprs:
        return "(no filters)"
    return " AND ".join(aFieldExprs)

def promptTimeRange():
    while True:
        sStart = input("Start time (HH:MM:SSZ): ").strip()
        sEnd = input("End time (HH:MM:SSZ): ").strip()
        if not (re.fullmatch(r"\d{2}:\d{2}:\d{2}Z", sStart) and re.fullmatch(r"\d{2}:\d{2}:\d{2}Z", sEnd)):
            print("Invalid time format, must match HH:MM:SSZ (example: 02:52:31Z).")
            continue
        if sStart > sEnd:
            print("Start time must be less than or equal to end time.")
            continue
        return (sStart, sEnd)

def promptDateRange():
    while True:
        sStart = input("Start date (YYYY-MM-DD): ").strip()
        sEnd = input("End date (YYYY-MM-DD): ").strip()
        if not (re.fullmatch(r"\d{4}-\d{2}-\d{2}", sStart) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", sEnd)):
            print("Invalid date format, must match YYYY-MM-DD (example: 2025-07-31).")
            continue
        if sStart > sEnd:
            print("Start date must be less than or equal to end date.")
            continue
        return (sStart, sEnd)

def mergeExpr(aList, tExpr, sWhat):
    if aList:
        sMode = chooseListMergeMode(sWhat, len(aList))
        if sMode == "k":
            print(sWhat + " kept.")
            return
        if sMode == "o":
            aList.clear()
        # if "a" fall-through to add
    aList.append(tExpr)
    print(sWhat + " set/updated.")
    print("Proposition: " + formatProposition())

def setTimeLogicOnce(sFieldName):
    while True:
        s = input(f"Combine multiple {sFieldName} ranges with (AND/OR): ").strip().upper()
        if s in ("AND", "OR"):
            return s
        print("Invalid logic. Choose AND or OR.")

# -----------------------------
# Main Menu Loop
# -----------------------------
while True:
    print("")
    print("Main Menu")
    print("1. Filter by Organization")
    print("2. Filter by Time Range (format HH:MM:SSZ)")
    print("3. Filter by Created_at Date Range (format YYYY-MM-DD)")
    print("4. Filter by Recipient")
    print("5. Filter by Requester ID")
    print("6. Filter by Result Type")
    print("7. Filter by Status")
    print("8. Filter by Subject")
    print("9. Filter by Submitter ID")
    print("10. Show Current Filter Proposition")
    print("11. Proceed with Retrieval")
    print("0. Exit")

    sChoice = input("Select an option: ").strip()

    if sChoice == "1":
        sInput = input("organization_id expression (e.g., (12345678912345 OR 12354678912345) AND 12364578912345): ").strip()
        tExpr = compileExpr(sInput, isValidOrgId14, "Invalid organization_id in expression. Each must be 14 digits.", bLower=False)
        if tExpr is not None:
            mergeExpr(aOrgExprs, tExpr, "Organization filter")

    elif sChoice == "2":
        if aTimeRanges:
            sMode = chooseListMergeMode("Time range filter", len(aTimeRanges))
            if sMode == "o":
                aTimeRanges.clear()
            if sMode in ("a","o"):
                aTimeRanges.append(promptTimeRange())
                # ask logic for time ranges whenever user selects this field
                sCreatedAtTimeLogic = setTimeLogicOnce("time")
                print("Time range filter set/updated.")
                print("Proposition: " + formatProposition())
            else:
                print("Time range filter kept.")
        else:
            aTimeRanges.append(promptTimeRange())
            sCreatedAtTimeLogic = setTimeLogicOnce("time")
            print("Time range filter set/updated.")
            print("Proposition: " + formatProposition())

    elif sChoice == "3":
        if aDateRanges:
            sMode = chooseListMergeMode("Date range filter", len(aDateRanges))
            if sMode == "o":
                aDateRanges.clear()
            if sMode in ("a","o"):
                aDateRanges.append(promptDateRange())
                sCreatedAtDateLogic = setTimeLogicOnce("date")
                print("Date range filter set/updated.")
                print("Proposition: " + formatProposition())
            else:
                print("Date range filter kept.")
        else:
            aDateRanges.append(promptDateRange())
            sCreatedAtDateLogic = setTimeLogicOnce("date")
            print("Date range filter set/updated.")
            print("Proposition: " + formatProposition())

    elif sChoice == "4":
        sInput = input("recipient expression (email; e.g., a@b.com OR c@d.com): ").strip()
        tExpr = compileExpr(sInput, isValidEmail, "Invalid recipient email in expression.", bLower=True)
        if tExpr is not None:
            mergeExpr(aRecipientExprs, tExpr, "Recipient filter")

    elif sChoice == "5":
        sInput = input("requester_id expression (digits; e.g., 1 OR 2 OR 3): ").strip()
        tExpr = compileExpr(sInput, isValidId, "Invalid requester_id in expression. Use digits only.", bLower=False)
        if tExpr is not None:
            mergeExpr(aRequesterIdExprs, tExpr, "Requester ID filter")

    elif sChoice == "6":
        sInput = input("result_type expression (e.g., ticket OR user): ").strip()
        tExpr = compileExpr(sInput, isValidResultType, "Invalid result_type in expression.", bLower=True)
        if tExpr is not None:
            mergeExpr(aResultTypeExprs, tExpr, "Result Type filter")

    elif sChoice == "7":
        sInput = input("status expression (new|open|pending|hold|solved|closed; e.g., open OR pending): ").strip()
        tExpr = compileExpr(sInput, isValidStatus, "Invalid status in expression.", bLower=True)
        if tExpr is not None:
            mergeExpr(aStatusExprs, tExpr, "Status filter")

    elif sChoice == "8":
        sInput = input("subject expression (contains; e.g., (urgent OR escalation) AND outage): ").strip()
        tExpr = compileExpr(sInput, isValidSubject, "Invalid subject value in expression. Each must be 1-200 characters.", bLower=True)
        if tExpr is not None:
            mergeExpr(aSubjectExprs, tExpr, "Subject filter")

    elif sChoice == "9":
        sInput = input("submitter_id expression (digits): ").strip()
        tExpr = compileExpr(sInput, isValidId, "Invalid submitter_id in expression. Use digits only.", bLower=False)
        if tExpr is not None:
            mergeExpr(aSubmitterIdExprs, tExpr, "Submitter ID filter")

    elif sChoice == "10":
        print("Proposition: " + formatProposition())

    elif sChoice == "11":
        break

    elif sChoice == "0":
        sys.exit(0)

    else:
        print("Invalid choice, please try again.")

aTicketList = []
harvestTickets("assigned",  f"{sZendeskBaseUrl}/api/v2/tickets.json?page[size]=100")
harvestSearch("cc",        f"type:ticket+cc:{nMyId}")
harvestSearch("follower",  f"type:ticket+follower:{nMyId}")
harvestSearch("requester", f"type:ticket+requester:{nMyId}")

aTicketList.sort(key=lambda d: d.get("id", 0)) # sort ascending by ID

# Apply filters here
aTicketList = applyFilters(aTicketList)

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
            oWs.merge_range(nRow, 0, nRow, 1, f"{sRole} - Ticket {nTicketId}", oFmtSection)
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
