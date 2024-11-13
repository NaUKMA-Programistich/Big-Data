import time
from math import log
from pyspark.sql import SparkSession
import re

# Constants
MIN_WORD_COUNT = 5
SHOW_COUNT = 10
DATASET_PATH = "dataset.csv"
OUTPUT_FILE = "top_words.txt"

spark = SparkSession.builder.appName("Reddit").getOrCreate()

def tokenize(text):
    text = text.replace('\n', ' ').replace('\t', ' ')
    text = re.sub(r'[^\w\s]', '', text)
    text = text.lower()
    tokens = text.split()
    tokens = [token for token in tokens if token.isalpha() and len(token) > 2]
    return tokens

def main():
    start_time = time.time()

    data = spark.read.csv(
        DATASET_PATH,
        header=True,
        multiLine=True,
        escape='"',
        quote='"'
    )
    print("------------------")
    print("First data from CSV:")
    data.show(SHOW_COUNT)

    data = data.withColumn('label', data['label'].cast('string'))
    print("------------------")
    print("Data after casting label to string:")
    data.show(SHOW_COUNT)

    posts = data.select('title', 'body', 'label').rdd
    print("------------------")
    print("Sample posts:")
    print(posts.take(SHOW_COUNT))

    # Part 1: MapReduce to compute ((word, label), N_wl) and (label, N_l)
    word_label_pairs = posts.flatMap(
        lambda row: [
            ((word, row['label']), 1)
            for word in tokenize((row['title'] or '') + ' ' + (row['body'] or ''))
        ]
    )
    print("------------------")
    print("Word-label pairs:")
    print(word_label_pairs.take(SHOW_COUNT))

    word_label_counts = word_label_pairs.reduceByKey(lambda x, y: x + y)
    print("------------------")
    print("Word-label counts:")
    print(word_label_counts.take(SHOW_COUNT))

    label_word_counts = posts.flatMap(
        lambda row: [
            (row['label'], 1)
            for _ in tokenize((row['title'] or '') + ' ' + (row['body'] or ''))
        ]
    ).reduceByKey(lambda x, y: x + y)
    print("------------------")
    print("Label word counts:")
    print(label_word_counts.take(SHOW_COUNT))

    label_word_counts_dict = dict(label_word_counts.collect())
    total_word_count = sum(label_word_counts_dict.values())

    label_word_counts_broadcast = spark.sparkContext.broadcast(label_word_counts_dict)
    total_word_count_broadcast = spark.sparkContext.broadcast(total_word_count)

    # Part 2: Compute total word counts N_w and filter frequent words
    word_total_counts = word_label_counts.map(lambda x: (x[0][0], x[1])).reduceByKey(lambda x, y: x + y)
    frequent_words = word_total_counts.filter(lambda x: x[1] >= MIN_WORD_COUNT).keys().collect()
    frequent_words_set = set(frequent_words)
    frequent_words_broadcast = spark.sparkContext.broadcast(frequent_words_set)

    word_to_label_counts = word_label_counts.map(lambda x: (x[0][0], (x[0][1], x[1])))
    word_grouped = word_to_label_counts.filter(
        lambda x: x[0] in frequent_words_broadcast.value
    ).groupByKey().mapValues(list)
    print("------------------")
    print("Word grouped by labels:")
    print(word_grouped.take(SHOW_COUNT))

    # Part 3: Compute Mutual Information (MI)
    def compute_mi(record):
        word, label_counts = record
        N_w = sum(count for _, count in label_counts)
        mi_list = []
        for label, N_wl in label_counts:
            N_l = label_word_counts_broadcast.value.get(label, 0)
            if N_l == 0:
                continue  # Avoid division by zero
            numerator = N_wl * total_word_count_broadcast.value
            denominator = N_w * N_l
            if numerator > 0 and denominator > 0:
                mi = log(numerator / denominator)
                mi_list.append((label, mi))
        return (word, mi_list)

    word_mi = word_grouped.map(compute_mi)
    print("------------------")
    print("Word MI:")
    print(word_mi.take(SHOW_COUNT))

    word_mi_flat = word_mi.flatMap(lambda x: [(x[0], label, mi) for label, mi in x[1]])
    print("------------------")
    print("Flattened Word MI:")
    print(word_mi_flat.take(SHOW_COUNT))

    target_label = '1'
    target_label_mi = word_mi_flat.filter(lambda x: x[1] == target_label)
    print("------------------")
    print(f"MI for label {target_label}:")
    print(target_label_mi.take(SHOW_COUNT))

    # Find the maximum MI value for the target label
    max_mi = target_label_mi.map(lambda x: x[2]).max()
    top_words = target_label_mi.filter(lambda x: x[2] == max_mi).collect()
    print("------------------")
    print(f"Top words for label {target_label} with MI={max_mi}:")

    for word_tuple in top_words:
        print(word_tuple)

    with open(OUTPUT_FILE, "w") as f:
        for word_tuple in top_words:
            f.write(word_tuple[0] + '\n')

    end_time = time.time()
    elapsed_minutes = (end_time - start_time) / 60
    print(f"Time taken: {elapsed_minutes:.2f} minutes")

    spark.stop()

if __name__ == "__main__":
    main()
