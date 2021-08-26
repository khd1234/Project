import sqlite3
import re
import zlib
try:
    import dateutil.parser as parser
except:
    pass


DNS_Mappings_dict = dict()
Mapping_Dict = dict()

def ModifySender(sender,All_senders_list=None):
    """
    Parses the sender email address, maps
    and return the cleaned mail address
    """
    global DNS_Mappings_dict
    global Mapping_Dict

    if sender is None:
        return None
    sender = sender.strip().lower()
    sender = sender.replace('<', '').replace('>', '')

    # Check if we have a hacked gmane.org from address
    if All_senders_list is not None and sender.endswith('gmane.org'):
        pieces = sender.split('-')
        realsender = None
        for s in All_senders_list:
            if s.startswith(pieces[0]):
                realsender = sender
                sender = s
                # print(realsender, sender)
                break
        if realsender is None:
            for s in Mapping_Dict:
                if s.startswith(pieces[0]):
                    realsender = sender
                    sender = Mapping_Dict[s]
                    # print(realsender, sender)
                    break
        if realsender is None:
            sender = pieces[0]

    mpieces = sender.split("@")
    if len(mpieces) != 2:
        return sender
    dns = mpieces[1]
    x = dns
    pieces = dns.split(".")
    if dns.endswith(".edu") or dns.endswith(".com") or dns.endswith(".org") or dns.endswith(".net"):
        dns = ".".join(pieces[-2:])
    else:
        dns = ".".join(pieces[-3:])
    # if dns != x : print(x,dns)
    # if dns != DNS_Mappings_dict.get(dns,dns) : print(dns,DNS_Mappings_dict.get(dns,dns))
    dns = DNS_Mappings_dict.get(dns, dns)
    return mpieces[0] + '@' + dns

def parsemaildate(md) :
    """
        Uncomment the code below if dateutil library is not present in the system
        Returns the data in required format
        """
    try:
        pdate = parser.parse(md)
        test_at = pdate.isoformat()
        return test_at
    except:
        pass

    # # Non-dateutil version - we try our best
    #
    # pieces = md.split()
    # notz = " ".join(pieces[:4]).strip()
    #
    # # Try a bunch of format variations - strptime() is *lame*
    # dnotz = None
    # for form in [ '%d %b %Y %H:%M:%S', '%d %b %Y %H:%M:%S',
    #     '%d %b %Y %H:%M', '%d %b %Y %H:%M', '%d %b %y %H:%M:%S',
    #     '%d %b %y %H:%M:%S', '%d %b %y %H:%M', '%d %b %y %H:%M' ] :
    #     try:
    #         dnotz = datetime.strptime(notz, form)
    #         break
    #     except:
    #         continue
    #
    # if dnotz is None :
    #     # print 'Bad Date:',md
    #     return None
    #
    # iso = dnotz.isoformat()
    #
    # tz = "+0000"
    # try:
    #     tz = pieces[4]
    #     ival = int(tz) # Only want numeric timezone values
    #     if tz == '-0000' : tz = '+0000'
    #     tzh = tz[:3]
    #     tzm = tz[3:]
    #     tz = tzh+":"+tzm
    # except:
    #     pass
    #
    # return iso+tz


# Parse out the info...
def parseheader(hdr, All_senders_list=None):
    """
    Parses the header
    Finds the email addresses, date, subject, Message-id from header
    Return Message-id, sender, subject and date as a tuple

    """
    if hdr is None or len(hdr) < 1:
        return None
    sender = None
    x = re.findall('\nFrom: .* <(\S+@\S+)>\n', hdr)
    if len(x) >= 1 :
        sender = x[0]
    else:
        x = re.findall('\nFrom: (\S+@\S+)\n', hdr)
        if len(x) >= 1 :
            sender = x[0]

    # normalize the domain name of Email addresses
    sender = ModifySender(sender, All_senders_list)

    date = None
    y = re.findall('\nDate: .*, (.*)\n', hdr)
    sent_at = None
    if len(y) >= 1:
        tdate = y[0]
        tdate = tdate[:26]
        try:
            sent_at = parsemaildate(tdate)
        except Exception as e:
            # print('Date ignored ',tdate, e)
            return None

    subject = None
    z = re.findall('\nSubject: (.*)\n', hdr)
    if len(z) >= 1:
        subject = z[0].strip().lower()

    guid = None
    z = re.findall('\nMessage-ID: (.*)\n', hdr)
    if len(z) >= 1:
        guid = z[0].strip().lower()

    if sender is None or sent_at is None or subject is None or guid is None:
        return None
    return (guid, sender, subject, sent_at)

conn = sqlite3.connect('index.sqlite')
cur = conn.cursor()

cur.execute('''DROP TABLE IF EXISTS Messages ''')
cur.execute('''DROP TABLE IF EXISTS Senders ''')
cur.execute('''DROP TABLE IF EXISTS Subjects ''')
cur.execute('''DROP TABLE IF EXISTS Replies ''')

cur.execute('''CREATE TABLE IF NOT EXISTS Messages
    (id INTEGER PRIMARY KEY, guid TEXT UNIQUE, sent_at INTEGER,
     sender_id INTEGER, subject_id INTEGER,
     headers BLOB, body BLOB)''')
cur.execute('''CREATE TABLE IF NOT EXISTS Senders
    (id INTEGER PRIMARY KEY, sender TEXT UNIQUE)''')
cur.execute('''CREATE TABLE IF NOT EXISTS Subjects
    (id INTEGER PRIMARY KEY, subject TEXT UNIQUE)''')


conn_1 = sqlite3.connect('mapping.sqlite')
cur_1 = conn_1.cursor()

#Creates a dictionary of DNSMapping and Mapping table in main memory
cur_1.execute('''SELECT old,new FROM DNSMapping''')
for message_row in cur_1:
    DNS_Mappings_dict[message_row[0].strip().lower()] = message_row[1].strip().lower()

Mapping_Dict = dict()
cur_1.execute('''SELECT old,new FROM Mapping''')
for message_row in cur_1:
    old = ModifySender(message_row[0])
    new = ModifySender(message_row[1])
    Mapping_Dict[old] = ModifySender(new)

# Done with mapping.sqlite
conn_1.close()

# Open the main content (Read only)
conn_1 = sqlite3.connect('file:content.sqlite?mode=ro', uri=True)
cur_1 = conn_1.cursor()

#Creates a list of all unique senders
All_senders_list = list()
cur_1.execute('''SELECT email FROM Messages''')
for message_row in cur_1:
    sender = ModifySender(message_row[0])
    if sender is None:
        continue
    if 'gmane.org' in sender:
        continue
    if sender in All_senders_list:
        continue
    All_senders_list.append(sender)


print("Loaded All_senders_list", len(All_senders_list), "and mapping", len(Mapping_Dict), "dns mapping", len(DNS_Mappings_dict))

cur_1.execute('''SELECT headers, body, sent_at
    FROM Messages ORDER BY sent_at''')

senders_dict = dict()
subjects_dict = dict()
guids_dict = dict()

count = 0

for message_row in cur_1:
    hdr = message_row[0]
    parsed = parseheader(hdr, All_senders_list)
    if parsed is None:
        continue
    (guid, sender, subject, sent_at) = parsed

    # Apply the sender mapping
    sender = Mapping_Dict.get(sender, sender)

    count = count + 1
    if count % 250 == 1:
        print(count, sent_at, sender)
    # print(guid, sender, subject, sent_at)

    if 'gmane.org' in sender:
        print("Error in sender ===", sender)

    sender_id = senders_dict.get(sender, None)
    subject_id = subjects_dict.get(subject, None)
    guid_id = guids_dict.get(guid, None)

    if sender_id is None:
        #Inserting to database and dictionary
        cur.execute('INSERT OR IGNORE INTO Senders (sender) VALUES ( ? )', (sender, ))
        conn.commit()
        cur.execute('SELECT id FROM Senders WHERE sender=? LIMIT 1', (sender, ))
        try:
            row = cur.fetchone()
            sender_id = row[0]
            senders_dict[sender] = sender_id
        except:
            print('Could not retrieve sender id', sender)
            break
    if subject_id is None:
        cur.execute('INSERT OR IGNORE INTO Subjects (subject) VALUES ( ? )', (subject, ))
        conn.commit()
        cur.execute('SELECT id FROM Subjects WHERE subject=? LIMIT 1', (subject, ))
        try:
            row = cur.fetchone()
            subject_id = row[0]
            subjects_dict[subject] = subject_id
        except:
            print('Could not retrieve subject id', subject)
            break
    # print(sender_id, subject_id)
    cur.execute('INSERT OR IGNORE INTO Messages (guid,sender_id, subject_id,sent_at, headers,body) VALUES ( ?,?,?, datetime(?),?,? )',
            (guid, sender_id, subject_id, sent_at,
            zlib.compress(message_row[0].encode()), zlib.compress(message_row[1].encode())))
    conn.commit()
    cur.execute('SELECT id FROM Messages WHERE guid=? LIMIT 1', (guid, ))
    try:
        row = cur.fetchone()
        message_id = row[0]
        guids_dict[guid] = message_id
    except:
        print('Could not retrieve guid id', guid)
        break

cur.close()
cur_1.close()
