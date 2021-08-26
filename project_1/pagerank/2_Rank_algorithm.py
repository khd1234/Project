import sqlite3

conn = sqlite3.connect('spider.sqlite')
cur = conn.cursor()

# Find the ids that send out page rank - we only are interested
# in pages in the SCC that have in and out links
cur.execute('''SELECT DISTINCT from_id FROM Links''')
From_ids_list = list()
for row in cur: 
    From_ids_list.append(row[0])


# Find the ids that receive page rank
To_ids_list = list()
Links_list = list()
cur.execute('''SELECT DISTINCT from_id, to_id FROM Links''')
for row in cur:
    from_id = row[0]
    to_id = row[1]
    if from_id == to_id:
        continue
    if from_id not in From_ids_list:
        continue
    if to_id not in From_ids_list:
        continue
    Links_list.append(row)
    if to_id not in To_ids_list:
        To_ids_list.append(to_id)


# Get latest page ranks for strongly connected component
prev_ranks_dict = dict()
for node in From_ids_list:
    cur.execute('''SELECT new_rank FROM Pages WHERE id = ?''', (node, ))
    row = cur.fetchone()
    prev_ranks_dict[node] = row[0]
# print('prev=', prev_ranks_dict)



no_iteration = input('How many iterations:')
many = 1
if len(no_iteration) > 0:
    many = int(no_iteration)

# Check
if len(prev_ranks_dict) < 1:
    print("Nothing to rank the page. Check data")
    quit()

# Adding to main memory for increasing speed
for i in range(many):
    # print prev_ranks_dict.items()[:5]
    next_ranks = dict()
    total = 0.0
    for (node, old_rank) in list(prev_ranks_dict.items()):
        total = total + old_rank
        next_ranks[node] = 0.0
    # print('total: ',total)
    # print("next_ranks", next_ranks)

    # Find the number of outbound links and sent the page rank down each
    for (node, old_rank) in list(prev_ranks_dict.items()):
        # print node, old_rank
        give_ids = list()
        for (from_id, to_id) in Links_list:
            if from_id != node:
                continue
           #  print '   ',from_id,to_id

            if to_id not in To_ids_list:
                continue
            give_ids.append(to_id)
        if len(give_ids) < 1:
            continue
        amount = old_rank / len(give_ids) #Oubound or shared
        # print(node, old_rank,amount, give_ids)
    
        for id in give_ids:
            next_ranks[id] = next_ranks[id] + amount  # Outbound amount received by linked node
    # print("next_ranks: ", next_ranks)

    newtot = 0
    #Taking a fraction away from everyone and giving it back to everybody else
    for (node, next_rank) in list(next_ranks.items()):
        newtot = newtot + next_rank
    evap = (total - newtot) / len(next_ranks)

    # print newtot, evap
    for node in next_ranks:
        next_ranks[node] = next_ranks[node] + evap
    # print("After evap next_ranks: ", next_ranks)

    newtot = 0
    for (node, next_rank) in list(next_ranks.items()):
        newtot = newtot + next_rank

    # Compute the per-page average change from old rank to new rank
    # As indication of convergence of the algorithm
    totdiff = 0
    for (node, old_rank) in list(prev_ranks_dict.items()):
        new_rank = next_ranks[node]
        diff = abs(old_rank-new_rank)
        totdiff = totdiff + diff

    avediff = totdiff / len(prev_ranks_dict)
    print("Iteration:", i+1, ' ', 'Deviation:', avediff)

    # Replaces previous ranks with new ranks
    prev_ranks_dict = next_ranks

# Put the final ranks back into the database
# print(list(next_ranks.items())[:5])
cur.execute('''UPDATE Pages SET old_rank=new_rank''')
for (id, new_rank) in list(next_ranks.items()):
    cur.execute('''UPDATE Pages SET new_rank=? WHERE id=?''', (new_rank, id))
conn.commit()
cur.close()
print('Run "create_json.py" to create the nodes for visualisation')

