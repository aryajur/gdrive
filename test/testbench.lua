require("submodsearcher")
require("debugUtil")
local json = require 'json'
local gd = require 'gdrive'

gdrive,msg = gd.new{
	creds_file = [[D:\Milind\Documents\creds.json]], 	-- Place the creds file if the file is used 
	-- Sample creds.json:
	--[[
	{
		"client_id":"CLIENT ID STRING",
		"auth_uri":"https://accounts.google.com/o/oauth2/auth",
		"token_uri":"https://accounts.google.com/o/oauth2/token",
		"auth_provider_x509_cert_url":"https://www.googleapis.com/oauth2/v1/certs",
		"client_secret":"CLIENT SECRET STRING",
		"redirect_uris":["urn:ietf:wg:oauth:2.0:oob","http://localhost"]
	}
	]]
	--[[ Use this block if creds.json file is not used,add the client id and secret  ]
	creds = {
		client_id = "CLIENT ID STRING",
		auth_uri = "https://accounts.google.com/o/oauth2/auth",
		token_uri = "https://accounts.google.com/o/oauth2/token",
		["auth_provider_x509_cert_url"] = "https://www.googleapis.com/oauth2/v1/certs",
		client_secret = "CLIENT SECRET STRING",
		redirect_uris = {
			"urn:ietf:wg:oauth:2.0:oob",
			"http://localhost"
		}
	}
	--[ Creds Block ends ]]
	tokens_file = 'tokens.json'
}


work = function()
	print('Acquire Token')
	--local stat = gdrive.oauth2:acquireToken()
	local stat = true
	local code,msg, folder, file, filebin, ret, content,meta
	if stat then
		print("Go to the following URL and grant permissions and get the authorization code:")
		--print(stat[1])
		print("Enter the authorization code:")
		--code = io.read()
		--stat,msg = stat[2](code)
		if not stat then
			print("Code authorization failed: "..msg)
		else
			print('Token acquired successfully.')
			print('Now trying the tests.')
			print('-- folder insertion')
			file = {title = 'test', mimeType = gdrive.mimeType.folder}
			folder,msg = gdrive:insert({}, file)
			if not folder then
				print("folder insertion failed: "..msg)
				return
			end
			print(t2s(folder))
			print('-- text file upload')
			file = {title = 'test', mimeType = gdrive.mimeType.file, parents = {{id = folder.id}}}
			ret, msg = gdrive:upload({}, file, string.format('os.time() = %d', os.time()))
			if not ret then
				print("File upload failed: "..msg)
			end
			print(t2s(ret))

			print('-- binary file upload')
			filebin = {title = 'testbin.bin', mimeType = gdrive.mimeType.file, parents = {{id = folder.id}}}
			local binData = ""
			for i = 1,100 do
				binData = binData..string.char(math.random(1,255))
			end
			ret, msg = gdrive:upload({}, filebin, binData)
			if not ret then
				print("File upload failed: "..msg)
			end
			print(t2s(ret))

			print('-- file listing')
			--[[ret = gdrive:list{
				maxResults = 10,
				q = string.format("mimeType = '%s' and title = '%s' and '%s' in parents", gdrive.mimeType.file, 'test', folder.id),
			}]]
			ret = gdrive:list({},gdrive.config.endpoint.."files/root/children")
			print(t2s(ret))
--[[
			print('-- file retrieval')
			content, meta = gdrive:get({}, ret.items[1].id)
			print('File content: ' .. content)
			print('File metadata: ' .. t2s(meta))

			print('-- binary file retrieval')
			content, meta = gdrive:get({}, ret.items[2].id)
			print('File content: ' .. content)
			print('File metadata: ' .. t2s(meta))
			assert(content == binData, "Binary file contents do not match")
			
			print("Press Enter to delete test directory.")
			io.read()
			print('-- files deletion')
			gdrive:delete({}, ret.items[1].id)
			gdrive:delete({}, ret.items[2].id)
			print('-- folder deletion')
			gdrive:delete({}, folder.id)
			print("delete the tokens.json file")
			os.remove(gdrive.config.tokens_file)	
			]]
			print("All done!")
		end
	else
		print('Acquisition failed: ' .. msg)
	end	
end

if not gdrive then
	print("Unable to initialize gdrive: "..msg)
else
	local status, err = work()
	if status then
		print('Operations completed successfully.')
	else
		print('Failure occurred: ' .. err)
	end
end
