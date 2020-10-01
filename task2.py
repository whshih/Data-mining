import json
import re
from pyspark import SparkContext
#2 from sys import argv


def tojson(line):
    lines = line.splitlines()
    data = json.loads(lines[0])
    return data


def map_catg(data):
    if data == None or data[0] == None or data[1] == None:
        return []
    out_list = []
    categories_list = data[0].split(",")
    for cate in categories_list:
        out_list.append([cate.strip(), data[1]])
    return out_list

def spark():
#def spark(argv):
    file_review = "review.json" #2 argv[1]
    file_business = "business.json" #2 argv[2]
    sc = SparkContext("local[*]", "task2")
    textRDD = sc.textFile(file_review).map(tojson)
    textRDD_busin = sc.textFile(file_business).map(tojson)
    # Compute the average stars for each business category and output top n categories with the highest average stars
    count = textRDD.map(lambda key: (
        key['business_id'], 1)).reduceByKey(lambda a, b: a + b)
    review_id = textRDD.map(lambda key: (key['business_id'], key['stars'])).reduceByKey(
        lambda a, b: a + b).join(count).map(lambda line: (line[0], line[1][0] / line[1][1]))
    catg = textRDD_busin.map(lambda key: (key['business_id'], key['categories'])).reduceByKey(
        lambda a, b: a + b).join(review_id).map(lambda line: (line[1][0], line[1][1])).flatMap(map_catg).map(
        lambda line: (line[0], (line[1], 1))).reduceByKey(
        lambda a, b: (a[0] + b[0], a[1] + b[1])).map(lambda line: (line[0], line[1][0] / line[1][1]))

    catg_sorted = sorted(
        catg.collect(), key=lambda key: (-key[1], key[0]), reverse=0)
    # string.strip(review_id)
    # print(catg_sorted[0:])
    return catg_sorted[0:10] # suppose n = 10
#2    return catg_sorted[0:int(argv[5])]



def main():
    data = {}
    a = "spark"
    if a == "spark":
#2    if argv[4] == "spark":
        data["result"] = spark()
#2        data["result"] = spark(argv)
    else:
        data["result"] = no_spark()
#2        data["result"] = no_spark(argv)
    with open("result_task2.json", "a") as fp_out:
#2    with open(argv[3], "a") as fp_out:
        json.dump(data, fp_out)

def no_spark():
#2 def no_spark(argv):

    review_data = []
    stars = {}

    # Reading the review.json file
    with open("review.json", "r", encoding="utf-8") as fd:
#2    with open(argv[1], "r", encoding="utf-8") as fd:
        raw = fd.read()
    for line in raw.splitlines():
        temp = json.loads(line)
        review_data.append(temp)  # list

    # Extracting business_id and stars from review.json
    list_key = []
    list_value = []
    for i in range(len(review_data)):
        list_key.append(review_data[i]['business_id'])
        list_value.append(float(review_data[i]['stars']))

    # Counting the stars & amount for every business_id
    dict1 = {}
    for i in range(len(list_key)):
        if list_key[i] in dict1:
            # Using Keys to build their value in tuple type --> dictionary={(key_name):(stars, amount)}
            dict1[list_key[i]] = (dict1[list_key[i]][0] +
                                  list_value[i], dict1[list_key[i]][1] + 1)
        else:
            dict1[list_key[i]] = (list_value[i], 1)

    # If there is no keys which you are looking for then build one in your dictionary,
    # and give it a name and count as 1 for the key.
    for i in dict1.keys():
        stars[i] = float(dict1[i][0] / dict1[i][1])

    # Reading the business.json
    business_data = []
    with open("business.json", "r", encoding="utf-8") as fp:
#2    with open(argv[2], "r", encoding="utf-8") as fp:
        raw2 = fp.read()
    for line in raw2.splitlines():
        temp = json.loads(line)  # dictionary
        business_data.append(temp)  # list of dict

    list_key2 = []
    list_value2 = []
    bus_catg = {}
    catg_stars = {}

    result = {}

    for i in range(len(business_data)):
        # Remove the 'None' value
        # if str(business_data[i]['categories']) != 'None':
        list_key2.append(business_data[i]['business_id'])
        list_value2.append(re.split(',', str(business_data[i]['categories'])))

    for i in range(len(list_value2)):
        for j in range(len(list_value2[i])):
            list_value2[i][j] = list_value2[i][j].strip()
    bus_catg = dict(zip(list_key2, list_value2))

    # Filter business IDs which exist in both stars and bus_catg dictionaries.
    # Firstly, drop the business IDs which have no rating stars in Stars dictionary.

    # Filtering the common IDs existing in both dictionaries by using Stars as standard.
    for y in stars.keys():
        if y in bus_catg:  # If IDs exist in Stars and bus_catg
            catg_stars[y] = (bus_catg[y], stars[y])  # Find the common IDs.

    # For separate categories and combine their rating stars.
    outlist = []
    for i in catg_stars.values():     # type       list             float
        # i = (category: pet/fun..., rating stars: 4...) i[0] = cate_stars
        for category in i[0]:
            outlist.append((category, i[1]))

    # outlist = [('Pizza', 4.3), ('Salad', 4.3),('Italian', 4.3)...]
    # catg_stars_final = {('Pizza',(99,22))}
    catg_stars_final = {}
    for i in range(len(outlist)):
        category_key = outlist[i][0]
        if category_key in catg_stars_final:
            # Using Keys to build their value in tuple type --> dictionary={(key_name):(stars, amount)}
            #         if isinstance(outlist[i][1],float):
            #             print(outlist[i])
            catg_stars_final[category_key] = (
                catg_stars_final[category_key][0] + outlist[i][1], catg_stars_final[category_key][1] + 1)
        else:
            catg_stars_final[category_key] = (outlist[i][1], 1)
    for category in catg_stars_final.keys():
        catg_stars_final[category] = (
            float(catg_stars_final[category][0]) / float(catg_stars_final[category][1]))
    outputResult = sorted(catg_stars_final.items(),
                          key=lambda key: (-key[1], key[0]), reverse=0)

    outputResult = outputResult[0:10] # suppose n = 10
#2    outputResult = outputResult[0:int(argv[5])]

    return outputResult


if __name__ == "__main__":
    main()
