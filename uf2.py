with open('rf2.sql', 'w') as outfile:
    with open('delete.1') as infile:
        for line in infile:
            i = line[:-2]
            assert "'" not in i
            sql = '''
delete from lineitem where L_ORDERKEY = {};
delete from orders   where O_ORDERKEY = {};
            '''.format(i, i).strip()
            print(sql, file=outfile)

