import os


def process(filename):
    print(filename)
    table = os.path.splitext(filename)[0]
    with open(filename + '.pop.sql', 'w') as outfile:
        with open(filename) as infile:
            for line in infile:
                assert "'" not in line
                values = line.split('|')[:-1]
                values = ["'{}'".format(v) for v in values]
                sql = "insert into {table} values ({values});".format_map({
                    'table': table,
                    'values': ','.join(values)
                })
                print(sql, file=outfile)


for root, dirs, files in os.walk(os.path.curdir):
    files = [fi for fi in files if fi.endswith(".tbl")]
    if files:
        assert len(files) == 8
        for file in files:
            process(file)
