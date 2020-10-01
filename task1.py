import json
import sys
import re
import io
from pyspark import SparkContext


def tojson(line):
    lines = line.splitlines()
    data = json.loads(lines[0])
    return data


def trans(datas):
    arr = []
    for data in datas:
        arr.append([data[1], data[0]])
    return arr


def trans2(input_list):
    result = []
    for element in input_list:
        result.append(element[1])
    return result


def sanitize(data, stopwords):
    words = re.findall("[a-zA-Z]+", data["text"])
    ret = []
    for word in words:
        word = word.lower()
        if word in stopwords:
            continue
        else:
            ret.append(word)
    return ret


def main():
    file = sys.argv[1]
    sc = SparkContext("local[*]", "task1")
    textRDD = sc.textFile(file).map(tojson)
    with open(sys.argv[3], "r") as fd:
        stopwords = fd.read().splitlines()

    # Task(1) A. Count the number of "date" then you can get the number of reviews in the file
    reviews_num = textRDD.filter(lambda x: 'date' in x).count()
    # Task(1) B. The number of reviews in a given year, y
    given_year = str(sys.argv[4])
    year = textRDD.filter(
        lambda y: 'date' in y and y["date"].startswith(given_year)).count()
    # year = textRDD.filter(lambda y: 'date' in y and y["date"].startswith("2014")).count()
    # Task(1) C. The number of distinct users who have written the reviews
    user = textRDD.map(lambda u:  u["user_id"]).distinct().count()
    # Task(1) D. Top "m" users who have the largest number of reviews and its count
    m = textRDD.map(lambda key: (key['user_id'], 1)).reduceByKey(
        lambda a, b: a + b).map(lambda key: (key[1], key[0])).sortByKey(False).collect()
    # Task(1) E.  Top "n" frequent words in the review text. The words should be in lower cases.
    n = textRDD.flatMap(lambda review: sanitize(review, stopwords)).map(lambda w: (w, 1)). reduceByKey(
        lambda a, b: a+b).map(lambda key: (key[1], key[0])).sortByKey(False).collect()

    # data = {"A: %i, B: %i, C: %i" % (reviews_num, year, user), ("D:", trans(m[0:sys.argv[5]])), ("E:", trans2(n[0:sys.argv[6]]))}
    # the index of list must be a integer. Convert the type by int() which can do: str -> int
    d = int(sys.argv[5])
    e = int(sys.argv[6])
    data = {}
    data["A"] = reviews_num
    data["B"] = year
    data["C"] = user
    data["D"] = trans(m[0:d])
    data["E"] = trans2(n[0:e])
    with io.open(sys.argv[2], "w") as fp_out:
        json.dump(data, fp_out)


if __name__ == "__main__":
    main()
