require "odbx"
require "print_r"
local db, err = odbx.init("pgsql")
local db, err = odbx.init("mysql", "127.0.0.1")
print_r(db)
print_r(db:error(0))
print_r(db:error_type(-1))



local err = db:bind('odbx', 'root')
--local err = db:bind_simple('odbx', 'postgres')
print_r(err)

local s = "'fdsafdsa'";
local a,b = db:escape(s)
print('========================')
local a,b = db:query("SELECT * FROM odbxtest")
print_r(a)
print_r(b)

local res = db:result()
print_r(res:rows_affected())
if res then 
	local fields = res:column_count()

	print_r(res:column_name())

	local e = res:row_fetch()
	while e do
		local t = {}
		for col=0,fields-1 do 
			local t1 = {}
			table.insert(t1, res:column_name(col))
			table.insert(t1, res:column_type(col))
			table.insert(t1, res:field_length(col))
			table.insert(t1, res:field_value(col))
			table.insert(t, t1)
		end
		print_r(t)
		print"-----------------------------------"
		e = res:row_fetch()
	end
	print"==========================================="
	res:finish()
end
print"**********************************************"
db:unbind()
db:finish()
