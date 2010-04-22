require "odbx"
require "print_r"
--local db, err = odbx.init("pgsql")
local db, err = odbx.init("mysql")
print_r(db)
print_r(db:error(0))
print_r(db:error_type(-1))



local err = db:bind('odbx', 'root')
--local err = db:bind_simple('odbx', 'postgres')
print_r(err)

local s = "'fdsafdsa'";
local a,b = db:escape(s)
print('========================')
local rt = 0
while true do
print("+++++++++" .. rt .. "+++++++++")
rt = rt + 1
db:query("insert into odbxtest(b) values(".. os.time() ..")")
local res,b = db:query("SELECT count(*) FROM odbxtest")
local res2,b = db:query("SELECT * FROM odbxtest where i=4")
print_r(res)
print_r(b)
--local res = db:result()
print_r(res:rows_affected())
if res then 
	local fields = res:column_count()
	print"s>"
	local e = res:row_fetch()
	print"s>"
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
		--print"-----------------------------------"
		e = res:row_fetch()
	end
	print"s>"
	--print_r(res:finish())
end
print"-------------------------------------"
print_r(res2:rows_affected())
if res then 
	local fields = res2:column_count()
	print"s>"
	local e = res2:row_fetch()
	print"s>"
	while e do
		local t = {}
		for col=0,fields-1 do 
			local t1 = {}
			table.insert(t1, res2:column_name(col))
			table.insert(t1, res2:column_type(col))
			table.insert(t1, res2:field_length(col))
			table.insert(t1, res2:field_value(col))
			table.insert(t, t1)
		end
		print_r(t)
		--print"-----------------------------------"
		e = res2:row_fetch()
	end
	print"s>"
	--print_r(res2:finish())
end
end

db:unbind()
db:finish()
