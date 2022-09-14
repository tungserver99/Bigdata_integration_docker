import sys
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--input_file", default=None, type=str, required=True, help="input file")
args = parser.parse_args()
corpus = args.input_file
    
model_path = "/data/model/"
text_df = spark.read.text(corpus)
indexer = StringIndexerModel.load(model_path + "string_indexer")
inverter = IndexToString(inputCol="prediction", outputCol="originalLabel", labels=indexer.labels)
indexed = indexer.transform(text_df)

only_str = indexed.withColumn("only_str",regexp_replace(col('value'), '\d+|“|”', ''))
only_letter = only_str.withColumn("only_letter",regexp_replace(col('only_str'), '\\p{Punct}', '')) 
regex_tokenizer = RegexTokenizer(inputCol="only_letter", outputCol="words")
raw_words = regex_tokenizer.transform(only_letter)

loadedCv = CountVectorizerModel.load(model_path + "/count_vectorizer")
countVectorizer_train = loadedCv.transform(raw_words)
idfModel = IDFModel.load(model_path + "/idf")
tf_idf_test = idfModel.transform(countVectorizer_train)

nb_save = model_path + "nb_model"
nbModel = NaiveBayesModel.load(nb_save)
nb_predictions = nbModel.transform(tf_idf_test)
nb_predictions = inverter.transform(nb_predictions)
category = nb_predictions.select("originalLabel").collect()[0][0]

print("Chủ đề của văn bản là: " + category)