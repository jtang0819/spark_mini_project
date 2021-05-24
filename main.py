from pyspark import SparkContext

sc = SparkContext("local", "My Application")
raw_rdd = sc.textFile('data.csv')


def extract_vin_key_value(line):
    split_line = line.split(',')
    # vin_number
    key = split_line[2]
    # make, year, incident type
    value = [split_line[3], split_line[5], split_line[1]]
    return key, value


def reset():
    master_make = ''
    master_year = ''


def populate_make(value):
    value = list(value)
    master_make = ''
    current_make = value[0]
    current_year = value[1]
    if current_make != master_make:
        if current_make is not None:
            return current_make, current_year
        reset()


vin_kv = raw_rdd.map(lambda x: extract_vin_key_value(x))
# print(vin_kv.collect())
enhance_make = vin_kv.groupByKey().flatMap(lambda kv: populate_make(kv[1]))
# print(enhance_make.collect())
print(enhance_make.collect())
# make_kv = enhance_make.map(lambda x: extract_make_key_value(x))
