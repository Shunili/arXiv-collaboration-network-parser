from xml.etree import ElementTree
import requests
import csv
import time

def adjustTag(tag):
	return "{{http://www.openarchives.org/OAI/2.0/}}{tag}".format(tag=tag)

def adjustTag2(tag):
	return "{{http://www.openarchives.org/OAI/2.0/oai_dc/}}{tag}".format(tag=tag)

def adjustTag3(tag):
	return "{{http://purl.org/dc/elements/1.1/}}{tag}".format(tag=tag)

data_source = "physics:nucl-th"
resumptionToken = None
authors_set = set()
num_edges = 0

count = 0
page_size = 1000
while (True):
	if resumptionToken == None:
		url = "https://export.arxiv.org/oai2?verb=ListRecords&from=2016-01-01&until=2016-06-31&set={}&metadataPrefix=oai_dc".format(data_source)
	else:
		url = "https://export.arxiv.org/oai2?verb=ListRecords&resumptionToken={}".format(resumptionToken)

	response = requests.get(url)
	response.raw.decode_content = True

	root = ElementTree.fromstring(response.content)

	records = root.findall(adjustTag("ListRecords"))
	assert len(records) == 1
	records = records[0]

	data = []

	for record in records:
		metadatas = record.findall(adjustTag("metadata"))
		for metadata in metadatas:
			dcs = metadata.findall(adjustTag2("dc"))		
			for dc in dcs:			
				authors = list(dc.findall(adjustTag3("creator")))			
				for i in range(len(authors) - 1):
					author_name_1 = authors[i].text

					if author_name_1 not in authors_set:
						authors_set.add(author_name_1)
					
					for j in range(i + 1, len(authors)):
						author_name_2 = authors[j].text
						edge = [author_name_1, author_name_2, "Undirected"]
						data.append(edge)

						if author_name_2 not in authors_set:
							authors_set.add(author_name_2)	

	print "Set {}:".format(count)
	print "Num of nodes: {}".format(len(authors_set))
	print "Num of new edges: {}".format(len(data))
	num_edges += len(data)
	print "Num of accum edges: {}".format(num_edges)

	encoded_data = [[unicode(n).encode("utf-8") for n in d] for d in data]

	file_path = "dataset-{}.csv".format(count)
	with open(file_path, "wb") as f:
	    writer = csv.writer(f, quoting=csv.QUOTE_ALL)
	    # writer.writerow(["Source", "Target", "Type"])
	    writer.writerows(encoded_data)

	# Get next trial token
	next_token = records.findall(adjustTag("resumptionToken"))
	assert len(next_token) == 1	
	if next_token[0].text:
		resumptionToken = next_token[0].text
	else:
		break

	# Wait for 10 sec before next request
	time.sleep(10)
	count += 1