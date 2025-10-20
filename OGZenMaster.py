from dotenv import load_dotenv
import os, sys, requests, csv, datetime, json, re, time, urllib.parse

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

# Ticket collectors that tag each ticket with its role (assigned / cc / follower / requester)
def harvestTickets(sRoleLabel, sStartUrl):
    global aTicketList, nBatchIndex, nTotalWritten
    sPage = sStartUrl
    while sPage:
        dJ = httpGetJson(sPage)
        for dT in dJ.get("tickets", []):
            dT["_role"] = sRoleLabel # stamp the role (internal only)
            aTicketList.append(dT)
            if len(aTicketList) >= 100:
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
                dHit["_role"] = sRoleLabel # stamp the role (internal only)
                aTicketList.append(dHit)
                if len(aTicketList) >= 100:
                    nTotalWritten += flushBatch()
        sPage = sNextLink(dJ)
    return

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
aDescriptionExprs = []

# Time/date ranges with per-field logic (AND/OR within field)
aTimeRanges = []
aDateRanges = []
sCreatedAtTimeLogic = "OR"
sCreatedAtDateLogic = "OR"

# Added: updated_at and due_at filters (time and date)
aUpdatedTimeRanges = []
aUpdatedDateRanges = []
sUpdatedAtTimeLogic = "OR"
sUpdatedAtDateLogic = "OR"

aDueTimeRanges = []
aDueDateRanges = []
sDueAtTimeLogic = "OR"
sDueAtDateLogic = "OR"

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

def isValidDescription(sVal):
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

def chooseExprLogicOnce(sFieldName):
    while True:
        s = input(f"Combine with existing filters using (AND/OR): ").strip().upper()
        if s in ("AND", "OR"):
            return s
        print("Invalid logic. Choose AND or OR.")

def choosePropositionMergeMode(nCount):
    while True:
        sMode = input(f"There is an existing filter. Choose: (a) add, (o) overwrite, (k) keep: ").strip().lower()
        if sMode in ("a","o","k"):
            return sMode
        print("Invalid choice. Enter a, o, or k.")

def addAtomWithMerge(sWhat, sDesc, fPred):
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
            print("Filter set/updated.")
            print("Proposition: " + formatProposition())
            return
        sOp = chooseExprLogicOnce(sWhat)
        aAtoms.append({"op": sOp, "desc": sDesc, "pred": fPred})
    else:
        aAtoms.append({"op": None, "desc": sDesc, "pred": fPred})
    print("Filter set/updated.")
    print("Proposition: " + formatProposition())

def mergeExpr(aList, tExpr, sWhat, sFieldKey):
    if tExpr is None:
        return
    aList.append(tExpr)
    sExpr, aRpn = tExpr
    if sFieldKey == "org":
        def fPred(dT, a=aRpn):
            return evalRpn(a, lambda v: str(dT.get("organization_id")) == v)
        addAtomWithMerge(sWhat, "(" + sExpr + ")", fPred)
    elif sFieldKey == "recipient":
        def fPred(dT, a=aRpn):
            return evalRpn(a, lambda v: str(dT.get("recipient") or "").lower() == v)
        addAtomWithMerge(sWhat, "(" + sExpr + ")", fPred)
    elif sFieldKey == "requester":
        def fPred(dT, a=aRpn):
            return evalRpn(a, lambda v: str(dT.get("requester_id")) == v)
        addAtomWithMerge(sWhat, "(" + sExpr + ")", fPred)
    elif sFieldKey == "result_type":
        def fPred(dT, a=aRpn):
            return evalRpn(a, lambda v: str(dT.get("result_type") or "").lower() == v)
        addAtomWithMerge(sWhat, "(" + sExpr + ")", fPred)
    elif sFieldKey == "status":
        def fPred(dT, a=aRpn):
            return evalRpn(a, lambda v: str(dT.get("status") or "").lower() == v)
        addAtomWithMerge(sWhat, "(" + sExpr + ")", fPred)
    elif sFieldKey == "subject":
        def fPred(dT, a=aRpn):
            return evalRpn(a, lambda v: v in str(dT.get("subject") or "").lower())
        addAtomWithMerge(sWhat, "(" + sExpr + ")", fPred)
    elif sFieldKey == "description":
        def fPred(dT, a=aRpn):
            return evalRpn(a, lambda v: v in str(dT.get("description") or "").lower())
        addAtomWithMerge(sWhat, "(" + sExpr + ")", fPred)
    elif sFieldKey == "submitter":
        def fPred(dT, a=aRpn):
            return evalRpn(a, lambda v: str(dT.get("submitter_id")) == v)
        addAtomWithMerge(sWhat, "(" + sExpr + ")", fPred)

aAtoms = []

# -----------------------------
# Main Menu Loop
# -----------------------------
def isValidIPv4(sVal):
    return isinstance(sVal, str) and re.fullmatch(r"(?:25[0-5]|2[0-4]\d|1?\d?\d)(?:\.(?:25[0-5]|2[0-4]\d|1?\d?\d)){3}", sVal) is not None

def isValidHash(sVal):
    return isinstance(sVal, str) and re.fullmatch(r"(?i)^[a-f0-9]{32}$|^[a-f0-9]{40}$|^[a-f0-9]{64}$", sVal) is not None

def isValidToken(sVal):
    return isinstance(sVal, str) and 1 <= len(sVal) <= 100

def customVal(dT, nId):
    try:
        for cf in dT.get("custom_fields", []):
            if int(cf.get("id") or 0) == int(nId):
                return cf.get("value")
    except Exception:
        return None
    return None

FIELD_IDS = {
    "Analyst": int(float("9.00003E+11")),
    "SeverityImpact": int(float("9.00006E+11")),
    "Site": int(float("1.9E+12")),
    "Classification": int(float("9E+11")),
    "SubClass": int(float("1.9E+12")),
    "DetectionThreatName": int(float("9.00013E+11")),
    "Username": int(float("1.9E+12")),
    "SourceIp": int(float("1.9E+12")),
    "DestinationIp": int(float("9.00012E+11")),
    "FileHash": int(float("1.9E+12")),
    "UrlWebsite": int(float("9.00013E+11")),
    "AnalystNotes": int(float("9.00012E+11")),
    "InitialResponseTime": int(float("9.00012E+11")),
    "RecommendationTime": int(float("9.00012E+11")),
}

def compileTokenExpr(sInput):
    return compileExpr(sInput, isValidToken, "Invalid value in expression.", bLower=True)

def compileIPv4Expr(sInput):
    return compileExpr(sInput, isValidIPv4, "Invalid IPv4 address in expression.", bLower=False)

def compileHashExpr(sInput):
    return compileExpr(sInput, isValidHash, "Invalid hash in expression. Use MD5/SHA1/SHA256 hex.", bLower=True)

def dtFromString_Ymd12h(sVal):
    try:
        return datetime.datetime.strptime(sVal, "%Y/%m/%d %I:%M %p")
    except Exception:
        return None

def addDropdownAtom(sLabel, nFieldId, sPrompt):
    sInput = input(sPrompt).strip()
    tExpr = compileTokenExpr(sInput)
    if tExpr is None:
        return
    sExpr, aRpn = tExpr
    def fPred(dT, a=aRpn, nid=nFieldId):
        v = str(customVal(dT, nid) or "").lower()
        return evalRpn(a, lambda tok: v == tok)
    addAtomWithMerge(sLabel, "(" + sExpr + ")", fPred)

def addContainsAtom(sLabel, nFieldId, sPrompt):
    sInput = input(sPrompt).strip()
    tExpr = compileExpr(sInput, isValidDescription, "Invalid value in expression. Each must be 1-200 characters.", bLower=True)
    if tExpr is None:
        return
    sExpr, aRpn = tExpr
    def fPred(dT, a=aRpn, nid=nFieldId):
        v = str(customVal(dT, nid) or "").lower()
        return evalRpn(a, lambda tok: tok in v)
    addAtomWithMerge(sLabel, "(" + sExpr + ")", fPred)

def addIPv4Atom(sLabel, nFieldId, sPrompt):
    sInput = input(sPrompt).strip()
    tExpr = compileIPv4Expr(sInput)
    if tExpr is None:
        return
    sExpr, aRpn = tExpr
    def fPred(dT, a=aRpn, nid=nFieldId):
        v = str(customVal(dT, nid) or "")
        return evalRpn(a, lambda tok: v == tok)
    addAtomWithMerge(sLabel, "(" + sExpr + ")", fPred)

def addHashAtom(sLabel, nFieldId, sPrompt):
    sInput = input(sPrompt).strip()
    tExpr = compileHashExpr(sInput)
    if tExpr is None:
        return
    sExpr, aRpn = tExpr
    def fPred(dT, a=aRpn, nid=nFieldId):
        v = str(customVal(dT, nid) or "").lower()
        return evalRpn(a, lambda tok: v == tok)
    addAtomWithMerge(sLabel, "(" + sExpr + ")", fPred)

def addTagsAtom():
    sInput = input("tags expression (e.g., (phishing OR malware) AND vip): ").strip()
    tExpr = compileTokenExpr(sInput)
    if tExpr is None:
        return
    sExpr, aRpn = tExpr
    def fPred(dT, a=aRpn):
        aTags = [str(t).lower() for t in (dT.get("tags") or [])]
        return evalRpn(a, lambda tok: tok in aTags)
    addAtomWithMerge("Tags filter", "(" + sExpr + ")", fPred)

def addStdStatusAtom():
    sInput = input("status expression (new|open|pending|hold|solved|closed; e.g., open OR pending): ").strip()
    tExpr = compileExpr(sInput, isValidStatus, "Invalid status in expression.", bLower=True)
    mergeExpr(aStatusExprs, tExpr, "Status filter", "status")

def addStdTypeAtom():
    sInput = input("type expression (e.g., incident OR problem OR task OR question): ").strip()
    tExpr = compileTokenExpr(sInput)
    if tExpr is None:
        return
    sExpr, aRpn = tExpr
    def fPred(dT, a=aRpn):
        return evalRpn(a, lambda tok: str(dT.get("type") or "").lower() == tok)
    addAtomWithMerge("Type filter", "(" + sExpr + ")", fPred)

def addStdAssigneeAtom():
    sInput = input("assignee_id expression (digits; e.g., 12345 OR 67890): ").strip()
    tExpr = compileExpr(sInput, isValidId, "Invalid assignee_id in expression. Use digits only.", bLower=False)
    if tExpr is None:
        return
    sExpr, aRpn = tExpr
    def fPred(dT, a=aRpn):
        return evalRpn(a, lambda tok: str(dT.get("assignee_id") or "") == tok)
    addAtomWithMerge("Assignee ID filter", "(" + sExpr + ")", fPred)

def addStdGroupAtom():
    sInput = input("group_id expression (digits; e.g., 111 OR 222): ").strip()
    tExpr = compileExpr(sInput, isValidId, "Invalid group_id in expression. Use digits only.", bLower=False)
    if tExpr is None:
        return
    sExpr, aRpn = tExpr
    def fPred(dT, a=aRpn):
        return evalRpn(a, lambda tok: str(dT.get("group_id") or "") == tok)
    addAtomWithMerge("Group ID filter", "(" + sExpr + ")", fPred)

def addStdSubjectAtom():
    sInput = input("subject expression (contains; e.g., (urgent OR escalation) AND outage): ").strip()
    tExpr = compileExpr(sInput, isValidSubject, "Invalid subject value in expression. Each must be 1-200 characters.", bLower=True)
    mergeExpr(aSubjectExprs, tExpr, "Subject filter", "subject")

def addStdDescriptionAtom():
    sInput = input("description expression (contains; e.g., (error OR failure) AND timeout): ").strip()
    tExpr = compileExpr(sInput, isValidDescription, "Invalid description value in expression. Each must be 1-200 characters.", bLower=True)
    mergeExpr(aDescriptionExprs, tExpr, "Description filter", "description")

def addDateTimeRangeAtom(sLabel, nFieldId):
    while True:
        sStart = input("Start (YYYY/MM/DD HH:MM AM/PM): ").strip()
        sEnd   = input("End   (YYYY/MM/DD HH:MM AM/PM): ").strip()
        if not (re.fullmatch(r"\d{4}/\d{2}/\d{2}\s\d{2}:\d{2}\s(?:AM|PM)", sStart) and re.fullmatch(r"\d{4}/\d{2}/\d{2}\s\d{2}:\d{2}\s(?:AM|PM)", sEnd)):
            print("Invalid datetime format, must match YYYY/MM/DD HH:MM AM/PM (example: 2025/09/10 07:45 PM).")
            continue
        oStart = dtFromString_Ymd12h(sStart)
        oEnd   = dtFromString_Ymd12h(sEnd)
        if not oStart or not oEnd or oStart > oEnd:
            print("Start must be <= End.")
            continue
        break
    def fPred(dT, nid=nFieldId, aStart=oStart, aEnd=oEnd):
        sVal = str(customVal(dT, nid) or "")
        oVal = dtFromString_Ymd12h(sVal)
        if not oVal:
            return False
        return aStart <= oVal <= aEnd
    addAtomWithMerge(sLabel, f'({sLabel} between "{sStart}" and "{sEnd}")', fPred)

while True:
    print("")
    print("Main Menu")
    print("1.  Filter by Analyst (drop-down exact match)")
    print("2.  Filter by Assignee ID")
    print("3.  Filter by Group ID")
    print("4.  Filter by Severity/Impact (drop-down)")
    print("5.  Filter by Status")
    print("6.  Filter by Tags")
    print("7.  Filter by Type (ticket type)")
    print("8.  Filter by Site (drop-down)")
    print("9.  Filter by Classification/Sub-Class (drop-down)")
    print("10. Filter by Detection/Threat Name (contains)")
    print("11. Filter by Username (contains)")
    print("12. Filter by Source IP Address (IPv4)")
    print("13. Filter by Destination IP Address (IPv4)")
    print("14. Filter by File Hash (MD5/SHA1/SHA256)")
    print("15. Filter by URL/Website (contains)")
    print("16. Filter by Subject (contains)")
    print("17. Filter by Description (contains)")
    print("18. Filter by Analyst Notes (contains)")
    print("19. Filter by Initial Response Time range (YYYY/MM/DD HH:MM AM/PM)")
    print("20. Filter by Recommendation Time range (YYYY/MM/DD HH:MM AM/PM)")
    print("21. Show Current Filter Proposition")
    print("22. Proceed with Retrieval")
    print("0.  Exit")

    sChoice = input("Select an option: ").strip()

    if sChoice == "1":
        addDropdownAtom("Analyst filter", FIELD_IDS["Analyst"], "analyst expression (e.g., analyst1 OR analyst2): ")
    elif sChoice == "2":
        addStdAssigneeAtom()
    elif sChoice == "3":
        addStdGroupAtom()
    elif sChoice == "4":
        addDropdownAtom("Severity/Impact filter", FIELD_IDS["SeverityImpact"], "severity/impact expression (e.g., high OR critical): ")
    elif sChoice == "5":
        addStdStatusAtom()
    elif sChoice == "6":
        addTagsAtom()
    elif sChoice == "7":
        addStdTypeAtom()
    elif sChoice == "8":
        addDropdownAtom("Site filter", FIELD_IDS["Site"], "site expression (e.g., hq OR dc1): ")
    elif sChoice == "9":
        addDropdownAtom("Classification filter", FIELD_IDS["Classification"], "classification expression (e.g., phishing OR malware): ")
        addDropdownAtom("Sub-Class filter", FIELD_IDS["SubClass"], "sub-class expression (e.g., credential_theft OR c2): ")
    elif sChoice == "10":
        addContainsAtom("Detection/Threat Name filter", FIELD_IDS["DetectionThreatName"], "detection/threat name expression (contains): ")
    elif sChoice == "11":
        addContainsAtom("Username filter", FIELD_IDS["Username"], "username expression (contains): ")
    elif sChoice == "12":
        addIPv4Atom("Source IP filter", FIELD_IDS["SourceIp"], "source ip expression (IPv4; e.g., 10.1.2.3 OR 203.0.113.10): ")
    elif sChoice == "13":
        addIPv4Atom("Destination IP filter", FIELD_IDS["DestinationIp"], "destination ip expression (IPv4; e.g., 10.1.2.3 OR 203.0.113.10): ")
    elif sChoice == "14":
        addHashAtom("File Hash filter", FIELD_IDS["FileHash"], "file hash expression (MD5/SHA1/SHA256; e.g., abc... OR def...): ")
    elif sChoice == "15":
        addContainsAtom("URL/Website filter", FIELD_IDS["UrlWebsite"], "url/website expression (contains): ")
    elif sChoice == "16":
        addStdSubjectAtom()
    elif sChoice == "17":
        addStdDescriptionAtom()
    elif sChoice == "18":
        addContainsAtom("Analyst Notes filter", FIELD_IDS["AnalystNotes"], "analyst notes expression (contains): ")
    elif sChoice == "19":
        addDateTimeRangeAtom("Initial Response Time", FIELD_IDS["InitialResponseTime"])
    elif sChoice == "20":
        addDateTimeRangeAtom("Recommendation Time", FIELD_IDS["RecommendationTime"])
    elif sChoice == "21":
        print("Proposition: " + formatProposition())
    elif sChoice == "22":
        break
    elif sChoice == "0":
        sys.exit(0)
    else:
        print("Invalid choice, please try again.")

aTicketList = []
sFileStamp = datetime.datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")

def cellValue(vRaw):
    if vRaw is None:
        return ""
    if isinstance(vRaw, (dict, list)):
        return json.dumps(vRaw, ensure_ascii=False) # JSON stays JSON
    if isinstance(vRaw, str):
        return vRaw.replace("\r", " ").replace("\n", " ")
    return vRaw # numbers / bools untouched

def writeBatchFiles(aTickets, nBatchIdx, bWantWorkbook):
    aTicketsSorted = sorted(aTickets, key=lambda d: d.get("id", 0))
    aColumnNames = sorted({k for dT in aTicketsSorted for k in dT.keys() if not k.startswith("_")})
    if "id" in aColumnNames:
        aColumnNames.remove("id")
    aColumnNames.insert(0, "id")
    sCsvFileName = f"zendesk_tickets_{sFileStamp}_batch_{nBatchIdx:05d}.csv"
    with open(sCsvFileName, "w", newline="", encoding="utf-8-sig") as hCsv:
        oCsvWriter = csv.DictWriter(
            hCsv,
            fieldnames=aColumnNames,
            extrasaction="ignore",
            quoting=csv.QUOTE_ALL,
            lineterminator="\r\n",
        )
        oCsvWriter.writeheader()
        for dT in aTicketsSorted:
            oCsvWriter.writerow({k: cellValue(dT.get(k)) for k in aColumnNames})
    sWorkbookName = None
    if bWantWorkbook:
        try:
            import xlsxwriter
        except ImportError:
            print("xlsxwriter not installed, skipping workbook.")
        else:
            sWorkbookName = f"zendesk_tickets_{sFileStamp}_batch_{nBatchIdx:05d}_formatted.xlsx"
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
                nRow += 3
            oWs.set_column(0, 0, 30,  oFmtField)
            oWs.set_column(1, 1, 100, oFmtValue)
            oWb.close()
    sEnvFileName = f"zendesk_tickets_{sFileStamp}_batch_{nBatchIdx:05d}.env"
    with open(sEnvFileName, "w", encoding="utf-8") as hEnv:
        for dT in aTicketsSorted:
            nId = dT.get("id")
            if nId is None:
                continue
            for k, v in dT.items():
                if k.startswith("_"):
                    continue
                sEnvVar = f'TICKET_{nId}_{re.sub(r"[^A-Za-z0-9]", "_", k).upper()}'
                hEnv.write(f'{sEnvVar}="{cellValue(v)}"\n')
    print(f"Wrote {len(aTicketsSorted)} tickets -> {sCsvFileName}")
    if bWantWorkbook and sWorkbookName:
        print(f"Wrote formatted workbook -> {sWorkbookName}")
    print(f"Wrote ticket-variable file -> {sEnvFileName}")
    return len(aTicketsSorted)

def flushBatch():
    global aTicketList, nBatchIndex, bMakeWorkbook
    if not aTicketList:
        return 0
    aBatch = aTicketList[:100]
    aTicketList = aTicketList[100:]
    aFiltered = applyFilters(aBatch)
    if not aFiltered:
        nBatchIndex += 1
        return 0
    nWritten = writeBatchFiles(aFiltered, nBatchIndex, bMakeWorkbook)
    nBatchIndex += 1
    return nWritten

# CSV for ingestion by Power BI
# Optional XLSX for formatted tickets as tables
bMakeWorkbook = input("Save formatted Excel workbook? (y/n): ").strip().lower() == "y"

nBatchIndex = 1
nTotalWritten = 0

harvestTickets("assigned",  f"{sZendeskBaseUrl}/api/v2/tickets.json?page[size]=100")
harvestSearch("cc",        f"type:ticket+cc:{nMyId}")
harvestSearch("follower",  f"type:ticket+follower:{nMyId}")
harvestSearch("requester", f"type:ticket+requester:{nMyId}")

nTotalWritten += flushBatch()

print(f"Total tickets written across batches: {nTotalWritten}")