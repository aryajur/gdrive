--require("debugUtil") -- module to modify the Lua searcher to find dependency modules in my setup

local gd = require 'gdrive'

gdrive,msg = gd.new{
	creds_file = [[creds.json]], 	-- creds file obtained from Google Developer console for this script
	tokens_file = 'tokens.json'		-- File name where the token will be saved
}

if not gdrive then
	print("Unable to initialize gdrive: "..msg)
else
	local stat, code, msg
	print('Acquire Token')
	stat = gdrive.acquireToken
	print("Go to the following URL and grant permissions and get the authorization code:")
	print(stat[1])
	print("Enter the authorization code:")
	code = io.read()
	stat,msg = stat[2](code)
	if not stat then
		print("Code authorization failed: "..msg)
	else
		print('Token acquired successfully.')
		print("The root directory listing is:")
		stat = gdrive.root:list()
		for i = 1,#stat.items do
			print(stat.items[i]:getProperty("title"),stat.items[i]:getProperty("mimeType")==gdrive.mimeType.folder and "folder" or "file")
		end
	end
end
