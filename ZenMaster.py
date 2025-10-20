from dotenv import load_dotenv
import os, sys, requests, csv, datetime, json, re, time, urllib.parse
from zoneinfo import ZoneInfo

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

oHttp = requests.Session()
oHttp.auth = tBasicAuth
oHttp.headers.update({"User-Agent": "ZenMaster/1.0", "Accept": "application/json"})
nDefaultTimeout = 30

def httpGetJson(sUrl, nMaxRetries=6):
    nTry = 0
    while True:
        nTry += 1
        try:
            oResp = oHttp.get(sUrl, timeout=nDefaultTimeout)
        except requests.RequestException as e:
            if nTry >= nMaxRetries:
                print(f"Network error contacting Zendesk: {e}")
                sys.exit(1)
            nSleep = min(2 ** (nTry - 1), 30)
            time.sleep(nSleep)
            continue

        nStatus = oResp.status_code

        if nStatus == 429:
            sRetryAfter = oResp.headers.get("Retry-After", "2")
            try:
                nSleep = max(1, int(float(sRetryAfter)))
            except Exception:
                nSleep = 2
            time.sleep(nSleep)
            if nTry >= nMaxRetries:
                print("Rate limited by Zendesk too many times (429).")
                sys.exit(1)
            continue

        if 500 <= nStatus < 600:
            if nTry >= nMaxRetries:
                print(f"Zendesk server error {nStatus}.")
                sys.exit(1)
            nSleep = min(2 ** (nTry - 1), 30)
            time.sleep(nSleep)
            continue

        if nStatus in (401, 403):
            try:
                dErr = oResp.json()
            except Exception:
                dErr = {}
            print(f"Authentication/authorization failed ({nStatus}). Check ZENDESK_SUBDOMAIN / ZENDESK_EMAIL / ZENDESK_API_TOKEN.")
            if dErr:
                print(json.dumps(dErr, ensure_ascii=False))
            sys.exit(1)

        try:
            oResp.raise_for_status()
        except requests.HTTPError as e:
            print(f"HTTP error from Zendesk: {e}")
            try:
                print(json.dumps(oResp.json(), ensure_ascii=False))
            except Exception:
                pass
            sys.exit(1)

        try:
            return oResp.json()
        except ValueError:
            print("Invalid JSON received from Zendesk.")
            sys.exit(1)

def sNextLink(dJ):
    sL = None
    try:
        sL = dJ.get("links", {}).get("next")
    except Exception:
        sL = None
    if not sL:
        sL = dJ.get("next_page")
    return sL

dMe = httpGetJson(f"{sZendeskBaseUrl}/api/v2/users/me.json")
if not isinstance(dMe, dict) or "user" not in dMe or "id" not in dMe["user"]:
    print("Unexpected response from /users/me.json")
    print(json.dumps(dMe, ensure_ascii=False))
    sys.exit(1)
nMyId = dMe["user"]["id"]

def minutesOfDay(oDt):
    return oDt.hour * 60 + oDt.minute

def parseTime12h(sVal):
    try:
        return datetime.datetime.strptime(sVal.strip(), "%I:%M %p").time()
    except Exception:
        return None

def inWindow(nMin, tStart, tEnd, bWrap):
    if bWrap:
        return (nMin >= tStart) or (nMin < tEnd)
    else:
        return (nMin >= tStart) and (nMin < tEnd)

def currentManilaShift():
    oNowPH = datetime.datetime.now(ZoneInfo("Asia/Manila"))
    nNow = minutesOfDay(oNowPH)
    # morning 06:30-18:30 (no wrap)
    # afternoon 01:30-13:30 (no wrap)
    # evening 21:30-09:30 (wrap)
    if inWindow(nNow, 6*60+30, 18*60+30, False):
        return "morning"
    if inWindow(nNow, 1*60+30, 13*60+30, False):
        return "afternoon"
    if inWindow(nNow, 21*60+30, 9*60+30, True):
        return "evening"
    return None

def shiftOfTime(sTime):
    oT = parseTime12h(sTime)
    if not oT:
        return None
    nMin = oT.hour*60 + oT.minute
    if inWindow(nMin, 6*60+30, 18*60+30, False):
        return "morning"
    if inWindow(nMin, 1*60+30, 13*60+30, False):
        return "afternoon"
    if inWindow(nMin, 21*60+30, 9*60+30, True):
        return "evening"
    return None

def shiftToWindows(sShift):
    if sShift == "morning":
        return [(6*60+30, 18*60+30, False)]
    if sShift == "afternoon":
        return [(1*60+30, 13*60+30, False)]
    if sShift == "evening":
        return [(21*60+30, 9*60+30, True)]
    return []

def parseDateExpr(sInput):
    s = sInput.strip()
    if not s:
        return []
    aParts = [p.strip() for p in s.split("OR")]
    aRanges = []
    for p in aParts:
        if "TO" not in p:
            return None
        a = [q.strip() for q in p.split("TO")]
        if len(a)!=2:
            return None
        try:
            d0 = datetime.date.fromisoformat(a[0])
            d1 = datetime.date.fromisoformat(a[1])
        except Exception:
            return None
        if d0 > d1:
            return None
        aRanges.append((d0.isoformat(), d1.isoformat()))
    return aRanges

def parseTimeExpr(sInput):
    s = (sInput or "").strip()
    if not s:
        sh = currentManilaShift()
        if not sh:
            return None, "Current Manila time is outside defined shifts."
        return [sh], None
    if "OR" in s:
        t1, t2 = [q.strip() for q in s.split("OR", 1)]
        sh1 = shiftOfTime(t1)
        sh2 = shiftOfTime(t2)
        if not sh1 or not sh2:
            return None, "Invalid time format. Use HH:MM AM/PM or HH:MM AM/PM OR HH:MM AM/PM."
        aOut = []
        for sh in [sh1, sh2]:
            if sh not in aOut:
                aOut.append(sh)
        return aOut, None
    else:
        sh = shiftOfTime(s)
        if not sh:
            return None, "Invalid time format. Use HH:MM AM/PM."
        return [sh], None

def compileExpr(sInput, fValidator, sErr, bLower=False):
    if not isinstance(sInput, str) or not sInput.strip():
        print(sErr)
        return None
    sNorm = re.sub(r"\s+", " ", sInput.strip())
    try:
        aTokens = tokenizeExpr(sNorm)
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
        if sInput[i:i+3].upper() == "AND" and (i+3 == n or sInput[i+3].isspace() or sInput[i+3] in "()"):
            aTokens.append("AND")
            i += 3
            continue
        if sInput[i:i+2].upper() == "OR" and (i+2 == n or sInput[i+2].isspace() or sInput[i+2] in "()"):
            aTokens.append("OR")
            i += 2
            continue
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

def choosePropositionMergeMode(nCount):
    while True:
        sMode = input(f"There is an existing filter. Choose: (a) add, (o) overwrite, (k) keep: ").strip().lower()
        if sMode in ("a","o","k"):
            return sMode
        print("Invalid choice. Enter a, o, or k.")

aAtoms = []

def applyFilters(aTickets):
    if not aAtoms:
        return aTickets
    aOut = []
    for dT in aTickets:
        bOk = aAtoms[0]["pred"](dT)
        for tAtom in aAtoms[1:]:
            if tAtom["op"] == "AND":
                bOk = bOk and tAtom["pred"](dT)
            else:
                bOk = bOk or tAtom["pred"](dT)
        if bOk:
            aOut.append(dT)
    return aOut

def formatProposition():
    if not aAtoms:
        return "(no filters)"
    sOut = aAtoms[0]["desc"]
    for tAtom in aAtoms[1:]:
        sOut += " " + tAtom["op"] + " " + tAtom["desc"]
    return sOut

def dtFromString_Ymd12h(sVal):
    try:
        return datetime.datetime.strptime(sVal, "%Y/%m/%d %I:%M %p")
    except Exception:
        return None

def addAtom_OR(sDesc, fPred):
    global aAtoms
    if aAtoms:
        sMode = choosePropositionMergeMode(len(aAtoms))
        if sMode == "k":
            print("Filter kept.")
            print("Proposition: " + formatProposition())
            return
        if sMode == "o":
            aAtoms = []
            aAtoms.append({"op": None, "desc": sDesc, "pred": fPred})
            print("Date/Time filter set.")
            print("Proposition: " + formatProposition())
            return
        aAtoms.append({"op": "OR", "desc": sDesc, "pred": fPred})
    else:
        aAtoms.append({"op": None, "desc": sDesc, "pred": fPred})
    print("Date/Time filter set.")
    print("Proposition: " + formatProposition())

def setDateTimeFilter():
    print("Date expression using ranges and AND/OR (Optional), sample:")
    print("2025-01-01 TO 2025-01-10 OR 2025-02-01 TO 2025-02-05")
    sDateExpr = input("> ").strip()
    aDateRanges = parseDateExpr(sDateExpr) if sDateExpr else []
    if aDateRanges is None:
        print("Invalid date expression.")
        return
    print("Time expression (single time or OR two times). Leave blank to use current Manila time.")
    print("Samples:")
    print("03:15 PM")
    print("03:15 PM OR 10:00 AM")
    sTimeExpr = input("> ").strip()
    aShifts, sErr = parseTimeExpr(sTimeExpr)
    if sTimeExpr=="" and sDateExpr=="":
        addAtom_OR("(all tickets)", lambda dT: True)
        return
    if sErr:
        print(sErr)
        if not aDateRanges:
            return
        aShifts = None
    def predDate(dT, a=aDateRanges):
        if not a:
            return True
        sCreated = dT.get("created_at")
        if not isinstance(sCreated, str) or "T" not in sCreated:
            return False
        sDate = sCreated.split("T")[0]
        for (s0, s1) in a:
            if s0 <= sDate <= s1:
                return True
        return False
    def predTime(dT, aSh=aShifts):
        if not aSh:
            return True
        sCreated = dT.get("created_at")
        if not isinstance(sCreated, str) or "T" not in sCreated:
            return False
        sTime = sCreated.split("T")[1]
        try:
            h, m, rest = sTime.split(":")
            sec = rest[:2]
            nMin = int(h)*60 + int(m)
        except Exception:
            return False
        for sh in aSh:
            for (st, en, wrap) in shiftToWindows(sh):
                if inWindow(nMin, st, en, wrap):
                    return True
        return False
    def fPred(dT):
        return predDate(dT) and predTime(dT)
    sDescParts = []
    if aDateRanges:
        sDescParts.append("(" + " OR ".join([f"{a[0]} TO {a[1]}" for a in aDateRanges]) + ")")
    if aShifts:
        sDescParts.append("(" + " OR ".join(aShifts) + ")")
    if not sDescParts:
        sDescParts.append("(all tickets)")
    sDesc = " AND ".join(sDescParts)
    addAtom_OR(sDesc, fPred)

FIELD_IDS = [
    900012377866, 900012444286, 14398053308057, 900013268543, 900013268523,
    900012385686, 900003199086, 900012385666, 1900001153468, 900000226446,
    1900001195968, 38672189313049, 900000247523, 900012385646, 19324908979225,
    900000226346, 19311219921561, 900012385626, 30141928341401, 30142024427545,
    900013261823, 1900001153268, 1900001166188, 900013268763, 900000226426,
    900012377846, 900012222846, 19278963058329, 19278922735769, 39525749101977,
    13762197131289, 35129962423321, 19278870990489, 19324776789657, 13762198754329,
    900012377626, 900011598866, 1900001153508, 1900001165728, 35130014125081,
    19278953511833, 900012399506, 900013284083, 900006185143, 1900001147108,
    900013268743, 900013268723, 1900001153228, 900000226366, 1900001147088,
    900000226326, 40814595742361, 900000452426, 900000226386, 900000623723,
    900013268923, 1900001153248, 26741419461785, 26739092941337, 26840523075609,
    26741958410393, 26738427296153, 26741776115865, 26838873374873, 26741944262681,
    26838913059353
]

CSV_HEADERS = [
    "ID",
    "Organization",
    "Field's / Custom Field's ID",
    "Action By Tool",
    "Action Taken",
    "Affected Asset",
    "Affected Asset [Delivered/Non-Remediated]",
    "Affected Asset [Quarantined/Remediated]",
    "Agent Name",
    "Analyst",
    "Analyst Notes",
    "Appliance Name",
    "Assignee",
    "Campaign",
    "Checker/Containment Action",
    "Classification",
    "Client/Action Response",
    "Closure Code",
    "Description",
    "Destination IP Address",
    "Destination URL",
    "Destination Website",
    "Detection/Threat Name",
    "Device Name/Serial Number",
    "File Hash",
    "File Name and Path",
    "Group",
    "Incident Evaluation",
    "Initial Response Time (YYYY/MM/DD HH:MM AM/PM)",
    "Linked Issues",
    "Opened by",
    "Overview",
    "Parent Image/Process",
    "Pending Action [(OWNER) Action]",
    "Pending Reason",
    "Problem Owner",
    "Process / Commandline",
    "Recommendation Time (YYYY/MM/DD HH:MM AM/PM)",
    "Reference Numbers",
    "Request Sub-Type",
    "Requestor",
    "Resolution Reason [(MM/DD) Reason]",
    "Root Cause",
    "Sender Address [Header From]",
    "Sender Address [SMTP]",
    "Severity/Impact",
    "Site",
    "Source Country",
    "Source Host Name",
    "Source IP Address",
    "Status",
    "Sub-Class",
    "Subject",
    "Tags",
    "Ticket Type",
    "Type",
    "Urgency",
    "URL/Website",
    "Username",
    "~Classification",
    "~Detections (K)",
    "~Detections (New)",
    "~Incident Validation",
    "~Mitre Att&ck Tactics and Techniques",
    "~Sub-Class (K)",
    "~Sub-Class (New)",
    "~Threat Name (K)",
    "~Threat Name (New)"
]

FIELD_NAME_MAP = [
    "Action By Tool","Action Taken","Affected Asset","Affected Asset [Delivered/Non-Remediated]",
    "Affected Asset [Quarantined/Remediated]","Agent Name","Analyst","Analyst Notes","Appliance Name",
    "Assignee","Campaign","Checker/Containment Action","Classification","Client/Action Response","Closure Code",
    "Description","Destination IP Address","Destination URL","Destination Website","Detection/Threat Name",
    "Device Name/Serial Number","File Hash","File Name and Path","Group","Incident Evaluation",
    "Initial Response Time (YYYY/MM/DD HH:MM AM/PM)","Linked Issues","Opened by","Overview","Parent Image/Process",
    "Pending Action [(OWNER) Action]","Pending Reason","Problem Owner","Process / Commandline",
    "Recommendation Time (YYYY/MM/DD HH:MM AM/PM)","Reference Numbers","Request Sub-Type","Requestor",
    "Resolution Reason [(MM/DD) Reason]","Root Cause","Sender Address [Header From]","Sender Address [SMTP]",
    "Severity/Impact","Site","Source Country","Source Host Name","Source IP Address","Status","Sub-Class","Subject",
    "Tags","Ticket Type","Type","Urgency","URL/Website","Username","~Classification","~Detections (K)",
    "~Detections (New)","~Incident Validation","~Mitre Att&ck Tactics and Techniques","~Sub-Class (K)","~Sub-Class (New)",
    "~Threat Name (K)","~Threat Name (New)"
]

STD_FIELD_GETTERS = {
    "ID": lambda dT: dT.get("id"),
    "Organization": lambda dT: dT.get("organization_id"),
    "Assignee": lambda dT: dT.get("assignee_id"),
    "Group": lambda dT: dT.get("group_id"),
    "Status": lambda dT: dT.get("status"),
    "Subject": lambda dT: dT.get("subject"),
    "Type": lambda dT: dT.get("type"),
    "Description": lambda dT: dT.get("description"),
    "Tags": lambda dT: ",".join([str(t) for t in (dT.get("tags") or [])]),
    "Ticket Type": lambda dT: dT.get("type")
}

def customVal(dT, nId):
    try:
        for cf in dT.get("custom_fields", []):
            if int(cf.get("id") or 0) == int(nId):
                return cf.get("value")
    except Exception:
        return None
    return None

def cellValue(vRaw):
    if vRaw is None:
        return ""
    if isinstance(vRaw, (dict, list)):
        return json.dumps(vRaw, ensure_ascii=False)
    if isinstance(vRaw, str):
        return vRaw.replace("\r", " ").replace("\n", " ")
    return vRaw

def ticketRow(dT):
    dRow = {}
    dRow["ID"] = STD_FIELD_GETTERS["ID"](dT)
    dRow["Organization"] = STD_FIELD_GETTERS["Organization"](dT)
    aIds = []
    for cf in (dT.get("custom_fields") or []):
        try:
            aIds.append(str(cf.get("id")))
        except Exception:
            pass
    dRow["Field's / Custom Field's ID"] = ",".join(aIds)
    for sKey in CSV_HEADERS:
        if sKey in ("ID","Organization","Field's / Custom Field's ID"):
            continue
        if sKey in STD_FIELD_GETTERS:
            dRow[sKey] = STD_FIELD_GETTERS[sKey](dT)
            continue
        try:
            idx = FIELD_NAME_MAP.index(sKey)
            nFieldId = FIELD_IDS[idx]
            v = customVal(dT, nFieldId)
            dRow[sKey] = v
        except Exception:
            dRow[sKey] = ""
    return dRow

def writeBatchFiles(aTickets, nBatchIdx, bWantWorkbook):
    aTicketsSorted = sorted(aTickets, key=lambda d: d.get("id", 0))
    oNowPH = datetime.datetime.now(ZoneInfo("Asia/Manila"))
    sStamp = oNowPH.strftime("%Y%m%d_%I%M%S_%p").lower()
    sCsvFileName = f"zendesk_tickets_{sStamp}_batch_{nBatchIdx:05d}.csv"
    with open(sCsvFileName, "w", newline="", encoding="utf-8-sig") as hCsv:
        oCsvWriter = csv.DictWriter(
            hCsv,
            fieldnames=CSV_HEADERS,
            extrasaction="ignore",
            quoting=csv.QUOTE_ALL,
            lineterminator="\r\n",
        )
        oCsvWriter.writeheader()
        for dT in aTicketsSorted:
            dRow = ticketRow(dT)
            oCsvWriter.writerow({k: cellValue(dRow.get(k)) for k in CSV_HEADERS})
    sWorkbookName = None
    if bWantWorkbook:
        try:
            import xlsxwriter
        except ImportError:
            print("xlsxwriter not installed, skipping workbook.")
        else:
            sWorkbookName = f"zendesk_tickets_{sStamp}_batch_{nBatchIdx:05d}_formatted.xlsx"
            oWb = xlsxwriter.Workbook(sWorkbookName, {"constant_memory": True})
            oWs = oWb.add_worksheet("tickets")
            oFmtSection = oWb.add_format({"bold": True, "align": "center", "valign": "vcenter", "bg_color": "#BDD7EE"})
            oFmtHead   = oWb.add_format({"bold": True, "border": 1, "text_wrap": True, "align": "center", "valign": "vcenter", "bg_color": "#D9E1F2"})
            oFmtField  = oWb.add_format({"border": 1, "text_wrap": True, "align": "center", "valign": "vcenter"})
            oFmtValue  = oWb.add_format({"border": 1, "text_wrap": True, "align": "left", "valign": "vcenter"})
            nRow = 0
            for dT in aTicketsSorted:
                nTicketId = dT.get("id", "UNKNOWN")
                sRole     = dT.get("_role", "unknown").upper()
                oWs.merge_range(nRow, 0, nRow, len(CSV_HEADERS)-1, f"{sRole} - Ticket {nTicketId}", oFmtSection)
                oWs.set_row(nRow, 20)
                nRow += 1
                for i, col in enumerate(CSV_HEADERS):
                    oWs.write(nRow, i, col, oFmtHead)
                oWs.set_row(nRow, 22)
                nRow += 1
                dRow = ticketRow(dT)
                for i, col in enumerate(CSV_HEADERS):
                    oWs.write(nRow, i, cellValue(dRow.get(col)), oFmtValue)
                oWs.set_row(nRow, 20)
                nRow += 2
            for i in range(len(CSV_HEADERS)):
                oWs.set_column(i, i, 28)
            oWb.close()
    print(f"Wrote {len(aTicketsSorted)} tickets -> {sCsvFileName}")
    if bWantWorkbook and sWorkbookName:
        print(f"Wrote formatted workbook -> {sWorkbookName}")
    return len(aTicketsSorted)

def flushBatch():
    global aTicketList, nBatchIndex, bMakeWorkbook
    if not aTicketList:
        return 0
    aBatch = aTicketList[:50]
    aTicketList = aTicketList[50:]
    aFiltered = applyFilters(aBatch)
    if not aFiltered:
        nBatchIndex += 1
        return 0
    nWritten = writeBatchFiles(aFiltered, nBatchIndex, bMakeWorkbook)
    nBatchIndex += 1
    return nWritten

def harvestTickets(sRoleLabel, sStartUrl):
    global aTicketList, nBatchIndex, nTotalWritten
    sPage = sStartUrl
    while sPage:
        dJ = httpGetJson(sPage)
        for dT in dJ.get("tickets", []):
            dT["_role"] = sRoleLabel
            aTicketList.append(dT)
            if len(aTicketList) >= 50:
                nTotalWritten += flushBatch()
        sPage = sNextLink(dJ)
    return

def harvestSearch(sRoleLabel, sQuery):
    global aTicketList, nBatchIndex, nTotalWritten
    sEncoded = urllib.parse.quote(sQuery, safe=":+")
    sPage = f"{sZendeskBaseUrl}/api/v2/search.json?query={sEncoded}&per_page=100"
    while sPage:
        dJ = httpGetJson(sPage)
        for dHit in dJ.get("results", []):
            if dHit.get("result_type") == "ticket":
                dHit["_role"] = sRoleLabel
                aTicketList.append(dHit)
                if len(aTicketList) >= 50:
                    nTotalWritten += flushBatch()
        sPage = sNextLink(dJ)
    return

def mainMenu():
    while True:
        print("")
        print("Main Menu")
        print("1.  Set/Change Date+Time filter")
        print("2.  Show Current Filter Proposition")
        print("3.  Proceed with Retrieval")
        print("0.  Exit")
        sChoice = input("Select an option: ").strip()
        if sChoice == "1":
            setDateTimeFilter()
        elif sChoice == "2":
            print("Proposition: " + formatProposition())
        elif sChoice == "3":
            break
        elif sChoice == "0":
            sys.exit(0)
        else:
            print("Invalid choice, please try again.")

mainMenu()

aTicketList = []
nBatchIndex = 1
nTotalWritten = 0

bMakeWorkbook = input("Save formatted Excel workbook? (y/n): ").strip().lower() == "y"

harvestTickets("assigned",  f"{sZendeskBaseUrl}/api/v2/tickets.json?page[size]=100")
harvestSearch("cc",        f"type:ticket+cc:{nMyId}")
harvestSearch("follower",  f"type:ticket+follower:{nMyId}")
harvestSearch("requester", f"type:ticket+requester:{nMyId}")

nTotalWritten += flushBatch()
while aTicketList:
    nTotalWritten += flushBatch()

print(f"Total tickets written across batches: {nTotalWritten}")
