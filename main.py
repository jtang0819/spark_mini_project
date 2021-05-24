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


def reset(x, y):
    master_make = x
    master_year = y


def populate_make(value):
    value = list(value)
    master_make = ''
    master_year = ''
    current_make = value[0]
    current_year = value[1]
    if current_make != master_make and current_year != master_year:
        #if line read is not equal to the master_make then-
        if current_make is not None:
            return current_make, current_year
            # this will return current make and year if it is a new group
        reset(current_make, current_year)
        # since it is a new group master_make and master_year will be set to current_make and master_year
    return current_make, current_year


vin_kv = raw_rdd.map(lambda x: extract_vin_key_value(x))
# print(vin_kv.collect())
enhance_make = vin_kv.groupByKey().flatMap(lambda kv: populate_make(kv[1]))
# print(enhance_make.collect())
print(enhance_make.collect())
# make_kv = enhance_make.map(lambda x: extract_make_key_value(x))
