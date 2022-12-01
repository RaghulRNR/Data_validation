print("--------------------------------------------------pagination_farm.py------------------------------")

from API_read_Farm import *
pagination=pagination.astype(str)
pagination=pagination.applymap(lambda x: x.strip())
#print(pagination['State'])
filter_pagination=pagination[(pagination['DCSRPS_STATE']==state_alpha)]

filter_pagination.reset_index(inplace=True,drop=True)
filter_pagination.sort_values(by=['DCSRPS_PAGE_NO'])
filter_pagination.reset_index(inplace=True)
#filter_pagination['index'] = filter_pagination['index'].astype(int)
print(filter_pagination)





