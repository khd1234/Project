import sqlite3
import time
import zlib
import string

conn = sqlite3.connect('index.sqlite')
cur = conn.cursor()

cur.execute('SELECT id, subject FROM Subjects')
#  Copying to main memory
subjects_dict = dict()
for message_row in cur:
    subjects_dict[message_row[0]] = message_row[1]

#Counting the word count of subjects
cur.execute('SELECT subject_id FROM Messages')
counts = dict()
for message_row in cur:
    text = subjects_dict[message_row[0]] #Selecting message stored using dict
    text = text.translate(str.maketrans('', '', string.punctuation))  # Dumps punctuation characters
    text = text.translate(str.maketrans('', '', '1234567890'))  # Dumps numbers
    text = text.strip()
    text = text.lower()
    words = text.split()
    for word in words:
        if len(word) < 4:
            continue
        counts[word] = counts.get(word, 0) + 1


x = sorted(counts, key=counts.get, reverse=True)
max = None
min = None
#  Finding maximum and minimum
for k in x[:100]:
    if max is None or max < counts[k]:
        max = counts[k]
    if min is None or min > counts[k]:
        min = counts[k]
print('Range of counts:', max, min)

# Spread the font sizes across 20-100 based on the count
bigsize = 80
smallsize = 20

#  Creating the json
fhand = open('gword.js', 'w')
fhand.write("gword = [")
first = True
for k in x[:100]:
    if not first:
        fhand.write(",\n")
    first = False
    #  Changing the size
    size = counts[k]
    size = (size - min) / float(max - min)
    size = int((size * bigsize) + smallsize)
    fhand.write("{text: '"+k+"', size: "+str(size)+"}")
fhand.write("\n];\n")
fhand.close()

print("OUPUT WRITTEN TO  gword.js")
print("OPEN gword.htm IN BROWSER FOR VISUALISATION")
