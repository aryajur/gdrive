require("debugUtil")
local json = require 'json'
local gd = require 'gdrive'

-- Things to test:
--[[
1. Create folder in the root
2. Create sub-folder in a folder
3. Create new text file
4. Create new binary file
5. Delete file
6. Delete folder
7. Download file
8. Download partial file
9. Get directory listing
10. Get the item
11. Move file
12. Move directory
13. Copy file
14. Copy folder
15. Rename file
16. Rename folder
17. Validate paths
]]


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

local ACQUIRE_TOKEN = true
local DELETE_TOKEN = true

work = function()
	local stat
	if ACQUIRE_TOKEN then
		print('Acquire Token')
		stat = gdrive.acquireToken
	else
		stat = true
	end
	local code,msg
	if stat then
		if ACQUIRE_TOKEN then
			print("Go to the following URL and grant permissions and get the authorization code:")
			print(stat[1])
			print("Enter the authorization code:")
			code = io.read()
			stat,msg = stat[2](code)
		end
		if not stat then
			print("Code authorization failed: "..msg)
			return nil
		else
			if ACQUIRE_TOKEN then
				print('Token acquired successfully.')
			end
			print('Now trying the tests.')
			print("-----------------------------------------------------")
			print("     CREATE FOLDERS ")
			print("-----------------------------------------------------")
			print("Create gd1/gd2")
			stat,msg = gdrive:mkdir([[gd1\gd2]])
			print("gd2 object", assert(stat,msg),assert(stat:getProperty("title")=="gd2"))
			local gd2 = stat
			stat,msg = gdrive:item("gd1","folder")
			print("gd1 object", assert(stat,msg),assert(stat:getProperty("title")=="gd1"))
			print("gd2 item object retrieval",assert((gdrive:item("gd1/gd2","folder")) == gd2))
			local gd1 = stat
			print("Create gd1/gd3")
			stat,msg = gd1:mkdir("gd3")
			print("gd3 object", assert(stat,msg),assert(stat:getProperty("title")=="gd3"))
			local gd3 = stat
			print("gd3 object modified date",gd3:getProperty("modifiedDate"))
			
			print("-----------------------------------------------------")
			print("     UPLOAD FILES ")
			print("-----------------------------------------------------")
			local file1 = "This is a text file\nThis is line 2 of text file."
			local file2 = ""
			for i = 1,100 do
				file2 = file2..string.char(math.random(0,255))
			end
			-- Upload text file in gd2
			local file1flag
			stat,msg = gd2:upload("file1.txt",function() if not file1flag then file1flag = true return file1 end end)
			print("file1 object", assert(stat,msg),assert(stat:getProperty("title")=="file1.txt"))
			local file1o = stat
			--print("file1 id",file1o:getProperty("id"))
			--print(file1o:getProperty("downloadUrl"))
			print("file1 object retrieval",assert((gdrive:item("gd1/gd2/file1.txt")) == file1o))
			
			-- Upload binary file in gd3
			local file2flag
			stat,msg = gd3:upload("file2.bin",function() if not file2flag then file2flag = true return file2 end end)
			print("file2 object", assert(stat,msg),assert(stat:getProperty("title")=="file2.bin"))
			local file2o = stat
			--print("file2 id",file2o:getProperty("id"))
			--print(file2o:getProperty("downloadUrl"))
			print("file2 object retrieval",assert((gdrive:item("gd1/gd3/file2.bin")) == file2o))

			print("-----------------------------------------------------")
			print("     DOWNLOAD AND VERIFY FILES ")
			print("-----------------------------------------------------")
			-- Download file1
			local file1d = ""
			stat,msg = file1o:download(function(data) file1d = file1d..data end)
			print("Compare file1 data to downloaded data",assert(stat,msg),assert(file1 == file1d))
			
			-- Download file2
			local file2d = ""
			stat,msg = file2o:download(function(data) file2d = file2d..data end)
			print("Compare file2 data to downloaded data",assert(stat,msg),assert(file2 == file2d))
			
			-- download portion of file 1
			file1d = ""
			stat,msg = file1o:download(function(data) file1d = file1d..data end,5,#file1-5)
			print(file1d)
			print("Compare file1 partial downloaded data",assert(stat,msg),assert(file1:sub(6,#file1-4)==file1d))

			-- download portion of file 2
			file2d = ""
			stat,msg = file2o:download(function(data) file2d = file2d..data end,5,#file2-5)
			print(file2d)
			print("Compare file2 partial downloaded data",assert(stat,msg),assert(file2:sub(6,#file2-4)==file2d))

			local function verifyList(list,expected)
				if #list.items ~= #expected then
					return nil
				end
				for i = 1,#expected do
					local found
					for j = 1,#list.items do
						if not expected[i][3] then
							if list.items[j]:getProperty("title") == expected[i][1] and ((expected[i][2] == "folder" and list.items[j]:getProperty("mimeType") == gdrive.mimeType.folder) or
							  (expected[i][2] ~= "folder" and list.items[j]:getProperty("mimeType") ~= gdrive.mimeType.folder)) then
								found = true
								expected[i][3] = true
								break
							end
						end
					end
					if not found then
						return nil
					end
				end
				return true
			end
			
			print("-----------------------------------------------------")
			print("     MOVE FILES/FOLDERS ")
			print("-----------------------------------------------------")
			
			-- move file1 to gd3
			print("Move file 1 from gd1/gd2 to gd1/gd3. Path before move: ",file1o:getProperty("path"),assert(file1o:getProperty("path")=="/gd1/gd2/","Object Path not valid"))
			stat,msg = file1o:move(gd3)
			print("Result of move.",assert(stat,msg),assert(file1o:getProperty("path")=="/gd1/gd3/","Object Path not valid"))
			print("Move file 2 from gd1/gd3 to gd1/gd3 to see response. Path before move: ", file2o:getProperty("path"),assert(file2o:getProperty("path")=="/gd1/gd3/","Object Path not valid"))
			stat,msg = file2o:move(gd3)
			assert("Result of move.",assert(not stat),msg,file2o:getProperty("path"),assert(file2o:getProperty("path")=="/gd1/gd3/","Object Path not valid"))
			
			-- Verify directory listings
			assert(verifyList(gd3:list(),{{"file1.txt"},{"file2.bin"}}),"gd3 directory listing incorrect.")
			
			-- move file1 back to gd2
			print("Move file 1 from gd1/gd3 to gd1/gd2. Path before move: ",file1o:getProperty("path"),assert(file1o:getProperty("path")=="/gd1/gd3/","Object Path not valid"))
			stat,msg = file1o:move(gd2)
			print("Result of move.",assert(stat,msg),assert(file1o:getProperty("path")=="/gd1/gd2/","Object Path not valid"))
			
			-- Verify directory listings
			assert(verifyList(gd3:list(),{{"file2.bin"}}),"gd3 directory listing incorrect.")
			-- Verify directory listings
			assert(verifyList(gd2:list(),{{"file1.txt"}}),"gd2 directory listing incorrect.")
			
			-- Create a same named file in gd3
			local file3flag
			stat,msg = gd3:upload("file1.txt",function() if not file3flag then file3flag = true return "This is the similar text file" end end)
			print("file3 object", assert(stat,msg),assert(stat:getProperty("title")=="file1.txt"))
			local file3o = stat
			
			-- Verify directory listings
			assert(verifyList(gd3:list(),{{"file1.txt"},{"file2.bin"}}),"gd3 directory listing incorrect.")
			
			-- Now try the move to gd3 again without force
			print("Move file 1 from gd1/gd2 to gd1/gd3 without force. Path before move: ",file1o:getProperty("path"),assert(file1o:getProperty("path")=="/gd1/gd2/","Object Path not valid"))
			stat,msg = file1o:move(gd3)
			print("Result of move.",assert(not stat),msg,assert(file1o:getProperty("path")=="/gd1/gd2/","Object Path not valid"))

			-- Verify directory listings
			assert(verifyList(gd3:list(),{{"file1.txt"},{"file2.bin"}}),"gd3 directory listing incorrect.")
			-- Verify directory listings
			assert(verifyList(gd2:list(),{{"file1.txt"}}),"gd2 directory listing incorrect.")
			
			-- Now try the move to gd3 again with force
			print("Move file 1 from gd1/gd2 to gd1/gd3 with force. Path before move: ",file1o:getProperty("path"),assert(file1o:getProperty("path")=="/gd1/gd2/","Object Path not valid"))
			stat,msg = file1o:move(gd3,true)
			print("Result of move.",assert(stat,msg),assert(file1o:getProperty("path")=="/gd1/gd3/","Object Path not valid"))
			
			-- Verify directory listings
			assert(verifyList(gd3:list(),{{"file1.txt"},{"file2.bin"}}),"gd3 directory listing incorrect.")
			-- Verify directory listings
			assert(verifyList(gd2:list(),{}),"gd2 directory listing incorrect.")
			
			print("file1 object retrieval",assert((gdrive:item("gd1/gd3/file1.txt")) == file1o))
			
			-- Verify directory listings
			assert(verifyList(gd1:list(),{{"gd2","folder"},{"gd3","folder"}}),"gd1 directory listing incorrect.")
			
			-- Move folder /gd1/gd3 to /gd1/gd2
			print("Move folder gd3 from /gd1 to /gd1/gd2. Path before move: ",gd3:getProperty("path"),assert(gd3:getProperty("path")=="/gd1/","Object Path not valid"))
			stat,msg = gd3:move(gd2)
			print("Result of move.",assert(stat,msg),assert(gd3:getProperty("path")=="/gd1/gd2/","Object Path not valid"))

			-- Verify directory listings
			assert(verifyList(gd1:list(),{{"gd2","folder"}}),"gd1 directory listing incorrect.")
			
			-- Verify directory listings
			assert(verifyList(gd2:list(),{{"gd3","folder"}}),"gd2 directory listing incorrect.")
			-- Verify directory listings
			assert(verifyList(gd3:list(),{{"file1.txt"},{"file2.bin"}}),"gd3 directory listing incorrect.")
			
			print("Move folder gd3 from /gd1/gd2 to /gd1/gd2 to see the response. Path before move: ",gd3:getProperty("path"),assert(gd3:getProperty("path")=="/gd1/gd2/","Object Path not valid"))
			stat,msg = gd3:move(gd2)
			print("Result of move.",assert(not stat),msg,assert(gd3:getProperty("path")=="/gd1/gd2/","Object Path not valid"))
			
			-- Verify directory listings
			assert(verifyList(gd1:list(),{{"gd2","folder"}}),"gd1 directory listing incorrect.")
			
			-- Verify directory listings
			assert(verifyList(gd2:list(),{{"gd3","folder"}}),"gd2 directory listing incorrect.")
			-- Verify directory listings
			assert(verifyList(gd3:list(),{{"file1.txt"},{"file2.bin"}}),"gd3 directory listing incorrect.")
			
			-- Move gd3 back to /gd1
			print("Move folder gd3 from /gd1/gd2 to /gd1. Path before move: ",gd3:getProperty("path"),assert(gd3:getProperty("path")=="/gd1/gd2/","Object Path not valid"))
			stat,msg = gd3:move(gd1)
			print("Result of move.",assert(stat,msg),assert(gd3:getProperty("path")=="/gd1/","Object Path not valid"))
			
			-- Verify directory listings
			assert(verifyList(gd1:list(),{{"gd2","folder"},{"gd3","folder"}}),"gd1 directory listing incorrect.")
			
			-- Verify directory listings
			assert(verifyList(gd2:list(),{}),"gd2 directory listing incorrect.")
			-- Verify directory listings
			assert(verifyList(gd3:list(),{{"file1.txt"},{"file2.bin"}}),"gd3 directory listing incorrect.")
			
			-- Create a same named folder in gd2
			print("Create gd1/gd2/gd3")
			stat,msg = gd2:mkdir("gd3")
			print("gd4 object", assert(stat,msg),assert(stat:getProperty("title")=="gd3"))
			local gd4 = stat
			
			-- Verify directory listings
			assert(verifyList(gd1:list(),{{"gd2","folder"},{"gd3","folder"}}),"gd1 directory listing incorrect.")
			
			-- Verify directory listings
			assert(verifyList(gd2:list(),{{"gd3","folder"}}),"gd2 directory listing incorrect.")
			-- Verify directory listings
			assert(verifyList(gd3:list(),{{"file1.txt"},{"file2.bin"}}),"gd3 directory listing incorrect.")
			
			-- Now try the move to gd2 again without force
			print("Move folder gd3 from /gd1 to /gd1/gd2 without force. Path before move: ",gd3:getProperty("path"),assert(gd3:getProperty("path")=="/gd1/","Object Path not valid"))
			stat,msg = gd3:move(gd2)
			print("Result of move.",assert(not stat),msg,assert(gd3:getProperty("path")=="/gd1/","Object Path not valid"))
			
			-- Verify directory listings
			assert(verifyList(gd1:list(),{{"gd2","folder"},{"gd3","folder"}}),"gd1 directory listing incorrect.")
			
			-- Verify directory listings
			assert(verifyList(gd2:list(),{{"gd3","folder"}}),"gd2 directory listing incorrect.")
			-- Verify directory listings
			assert(verifyList(gd3:list(),{{"file1.txt"},{"file2.bin"}}),"gd3 directory listing incorrect.")
			
			-- Now try the move to gd2 again with force
			print("Move folder gd3 from /gd1 to /gd1/gd2 with force. Path before move: ",gd3:getProperty("path"),assert(gd3:getProperty("path")=="/gd1/","Object Path not valid"))
			stat,msg = gd3:move(gd2,true)
			print("Result of move.",assert(stat,msg),assert(gd3:getProperty("path")=="/gd1/gd2/","Object Path not valid"))
			
			-- Verify directory listings
			assert(verifyList(gd1:list(),{{"gd2","folder"}}),"gd1 directory listing incorrect.")
			
			-- Verify directory listings
			assert(verifyList(gd2:list(),{{"gd3","folder"}}),"gd2 directory listing incorrect.")
			-- Verify directory listings
			assert(verifyList(gd3:list(),{{"file1.txt"},{"file2.bin"}}),"gd3 directory listing incorrect.")
			
			-- roll back to carry same operations in copy
			stat,msg = gd3:move(gd1)
			assert(stat,msg)
			stat,msg = file1o:move(gd2)
			assert(stat,msg)

			-- Verify directory listings
			assert(verifyList(gd1:list(),{{"gd2","folder"},{"gd3","folder"}}),"gd1 directory listing incorrect.")			
			-- Verify directory listings
			assert(verifyList(gd3:list(),{{"file2.bin"}}),"gd3 directory listing incorrect.")
			-- Verify directory listings
			assert(verifyList(gd2:list(),{{"file1.txt"}}),"gd2 directory listing incorrect.")
			
			print("-----------------------------------------------------")
			print("     COPY FILES ")
			print("-----------------------------------------------------")
			print("File 1 id: ",file1o:getProperty("id"))
			print("gd3 id: ",gd3:getProperty("id"))
			--io.read()
			-- copy file1 to gd3
			print("Copy file 1 from gd1/gd2 to gd1/gd3. Path before move: ",file1o:getProperty("path"),assert(file1o:getProperty("path")=="/gd1/gd2/","Object Path not valid"))
			stat,msg = file1o:copyto(gd3)
			print("Result of copy.",assert(stat,msg),assert(stat:getProperty("path")=="/gd1/gd3/","Object Path not valid"))
			local file1oc = stat
			
			-- Verify directory listings
			assert(verifyList(gd1:list(),{{"gd2","folder"},{"gd3","folder"}}),"gd1 directory listing incorrect.")			
			-- Verify directory listings
			assert(verifyList(gd2:list(),{{"file1.txt"}}),"gd2 directory listing incorrect.")
			-- Verify directory listings
			assert(verifyList(gd3:list(),{{"file1.txt"},{"file2.bin"}}),"gd3 directory listing incorrect.")
			
			print("Copy file 2 from gd1/gd3 to gd1/gd3 to see response. Path before move: ", file2o:getProperty("path"),assert(file2o:getProperty("path")=="/gd1/gd3/","Object Path not valid"))
			stat,msg = file2o:copyto(gd3)
			assert("Result of copy.",assert(not stat),msg,assert(file2o:getProperty("path")=="/gd1/gd3/","Object Path not valid"))
			
			-- Verify directory listings
			assert(verifyList(gd1:list(),{{"gd2","folder"},{"gd3","folder"}}),"gd1 directory listing incorrect.")			
			-- Verify directory listings
			assert(verifyList(gd2:list(),{{"file1.txt"}}),"gd2 directory listing incorrect.")
			-- Verify directory listings
			assert(verifyList(gd3:list(),{{"file1.txt"},{"file2.bin"}}),"gd3 directory listing incorrect.")
			
			-- delete file1 copy
			print("Delete file 1 copy.",file1oc:delete())
			
			-- Verify directory listings
			assert(verifyList(gd1:list(),{{"gd2","folder"},{"gd3","folder"}}),"gd1 directory listing incorrect.")			
			-- Verify directory listings
			assert(verifyList(gd3:list(),{{"file2.bin"}}),"gd3 directory listing incorrect.")
			-- Verify directory listings
			assert(verifyList(gd2:list(),{{"file1.txt"}}),"gd2 directory listing incorrect.")
			
			-- Create a same named file in gd3
			file3flag = nil
			stat,msg = gd3:upload("file1.txt",function() if not file3flag then file3flag = true return "This is the similar text file" end end)
			print("file3 object", assert(stat,msg),assert(stat:getProperty("title")=="file1.txt"))
			file3o = stat
			
			-- Verify directory listings
			assert(verifyList(gd1:list(),{{"gd2","folder"},{"gd3","folder"}}),"gd1 directory listing incorrect.")			
			-- Verify directory listings
			assert(verifyList(gd2:list(),{{"file1.txt"}}),"gd2 directory listing incorrect.")
			-- Verify directory listings
			assert(verifyList(gd3:list(),{{"file1.txt"},{"file2.bin"}}),"gd3 directory listing incorrect.")
			
			-- Now try the copy to gd3 again without force
			print("Copy file 1 from gd1/gd2 to gd1/gd3 without force. Path before move: ",file1o:getProperty("path"),assert(file1o:getProperty("path")=="/gd1/gd2/","Object Path not valid"))
			stat,msg = file1o:copyto(gd3)
			print("Result of copy.",assert(not stat),msg)

			-- Verify directory listings
			assert(verifyList(gd1:list(),{{"gd2","folder"},{"gd3","folder"}}),"gd1 directory listing incorrect.")			
			-- Verify directory listings
			assert(verifyList(gd2:list(),{{"file1.txt"}}),"gd2 directory listing incorrect.")
			-- Verify directory listings
			assert(verifyList(gd3:list(),{{"file1.txt"},{"file2.bin"}}),"gd3 directory listing incorrect.")
			
			-- Now try the copy to gd3 again with force
			print("Copy file 1 from gd1/gd2 to gd1/gd3 with force. Path before move: ",file1o:getProperty("path"),assert(file1o:getProperty("path")=="/gd1/gd2/","Object Path not valid"))
			stat,msg = file1o:copyto(gd3,true)
			print("Result of copy.",assert(stat,msg),assert(stat:getProperty("path")=="/gd1/gd3/","Object Path not valid"))
			file1oc = stat
			file1d = ""
			stat,msg = stat:download(function(data) file1d = file1d..data end)
			print("Compare file1 data to downloaded data",assert(stat,msg),assert(file1 == file1d))
			
			-- Verify directory listings
			assert(verifyList(gd1:list(),{{"gd2","folder"},{"gd3","folder"}}),"gd1 directory listing incorrect.")			
			-- Verify directory listings
			assert(verifyList(gd2:list(),{{"file1.txt"}}),"gd2 directory listing incorrect.")
			-- Verify directory listings
			assert(verifyList(gd3:list(),{{"file1.txt"},{"file2.bin"}}),"gd3 directory listing incorrect.")
			
			print("gd2 id: ",gd2:getProperty("id"))
			
			-- Copy folder /gd1/gd3 to /gd1/gd2
			print("Copy folder gd3 from /gd1 to /gd1/gd2. Path before move: ",gd3:getProperty("path"),assert(gd3:getProperty("path")=="/gd1/","Object Path not valid"))
			stat,msg = gd3:copyto(gd2)
			print("Result of copy.",assert(not stat),msg)

			print("-----------------------------------------------------")
			print("     RENAME FILES/FOLDERS ")
			print("-----------------------------------------------------")
			-- Rename file1.txt in gd3 to file2.txt
			stat,msg = file1oc:rename("file2.txt")
			print("Result of rename.",assert(stat,msg))
			
			-- Verify directory listings
			assert(verifyList(gd1:list(),{{"gd2","folder"},{"gd3","folder"}}),"gd1 directory listing incorrect.")			
			-- Verify directory listings
			assert(verifyList(gd2:list(),{{"file1.txt"}}),"gd2 directory listing incorrect.")
			-- Verify directory listings
			assert(verifyList(gd3:list(),{{"file2.txt"},{"file2.bin"}}),"gd3 directory listing incorrect.")
			
			-- Try renaming file2.bin to file2.txt without force
			stat,msg = file2o:rename("file2.txt")
			print("Result of rename.",assert(not stat),msg)
			
			-- Verify directory listings
			assert(verifyList(gd1:list(),{{"gd2","folder"},{"gd3","folder"}}),"gd1 directory listing incorrect.")			
			-- Verify directory listings
			assert(verifyList(gd2:list(),{{"file1.txt"}}),"gd2 directory listing incorrect.")
			-- Verify directory listings
			assert(verifyList(gd3:list(),{{"file2.txt"},{"file2.bin"}}),"gd3 directory listing incorrect.")
			
			-- Rename file2.bin to file2.bin
			stat,msg = file2o:rename("file2.bin")
			print("Result of rename.",assert(stat,msg))
			
			-- Verify directory listings
			assert(verifyList(gd1:list(),{{"gd2","folder"},{"gd3","folder"}}),"gd1 directory listing incorrect.")			
			-- Verify directory listings
			assert(verifyList(gd2:list(),{{"file1.txt"}}),"gd2 directory listing incorrect.")
			-- Verify directory listings
			assert(verifyList(gd3:list(),{{"file2.txt"},{"file2.bin"}}),"gd3 directory listing incorrect.")
			
			-- Rename file2.bin to file2.txt with force
			stat,msg = file2o:rename("file2.txt",true)
			print("Result of rename.",assert(stat,msg))
			
			-- Verify directory listings
			assert(verifyList(gd1:list(),{{"gd2","folder"},{"gd3","folder"}}),"gd1 directory listing incorrect.")			
			-- Verify directory listings
			assert(verifyList(gd2:list(),{{"file1.txt"}}),"gd2 directory listing incorrect.")
			-- Verify directory listings
			assert(verifyList(gd3:list(),{{"file2.txt"}}),"gd3 directory listing incorrect.")
			
			-- verify the content of file2.txt
			-- Download file2
			file2d = ""
			stat,msg = file2o:download(function(data) file2d = file2d..data end)
			print("Compare file2 data to downloaded data",assert(stat,msg),assert(file2 == file2d))
			
			-- Rename gd3 in gd4
			stat,msg = gd3:rename("gd4")
			print("Result of rename.",assert(stat,msg))
			
			-- Verify directory listings
			assert(verifyList(gd1:list(),{{"gd2","folder"},{"gd4","folder"}}),"gd1 directory listing incorrect.")			
			-- Verify directory listings
			assert(verifyList(gd2:list(),{{"file1.txt"}}),"gd2 directory listing incorrect.")
			-- Verify directory listings
			assert(verifyList(gd3:list(),{{"file2.txt"}}),"gd3 directory listing incorrect.")
			
			-- Try renaming gd4 to gd2 without force
			stat,msg = gd3:rename("gd2")
			print("Result of rename.",assert(not stat),msg)
			
			-- Verify directory listings
			assert(verifyList(gd1:list(),{{"gd2","folder"},{"gd4","folder"}}),"gd1 directory listing incorrect.")			
			-- Verify directory listings
			assert(verifyList(gd2:list(),{{"file1.txt"}}),"gd2 directory listing incorrect.")
			-- Verify directory listings
			assert(verifyList(gd3:list(),{{"file2.txt"}}),"gd3 directory listing incorrect.")
			
			-- Rename gd4 to gd4
			stat,msg = gd3:rename("gd4")
			print("Result of rename.",assert(stat,msg))
			
			-- Verify directory listings
			assert(verifyList(gd1:list(),{{"gd2","folder"},{"gd4","folder"}}),"gd1 directory listing incorrect.")			
			-- Verify directory listings
			assert(verifyList(gd2:list(),{{"file1.txt"}}),"gd2 directory listing incorrect.")
			-- Verify directory listings
			assert(verifyList(gd3:list(),{{"file2.txt"}}),"gd3 directory listing incorrect.")
			
			-- Rename gd4 to gd2 with force
			stat,msg = gd3:rename("gd2",true)
			print("Result of rename.",assert(stat,msg))
			
			-- Verify directory listings
			assert(verifyList(gd1:list(),{{"gd2","folder"}}),"gd1 directory listing incorrect.")			
			-- Verify directory listings
			assert(verifyList(gd3:list(),{{"file2.txt"}}),"gd3 directory listing incorrect.")
			
			print("-----------------------------------------------------")
			print("     DELETE FOLDERS ")
			print("-----------------------------------------------------")
			stat,msg = gd3:delete()
			assert(stat,msg)
			stat,msg = gd1:delete()
			assert(stat,msg)
			if DELETE_TOKEN then
				print("delete the tokens.json file")
				os.remove(gdrive.config.tokens_file)	
			end
			print("All done!")
		end
	else
		print('Token Acquisition failed: ' .. msg)
		return nil
	end	
	return true
end

if not gdrive then
	print("Unable to initialize gdrive: "..msg)
else
	local status, err = work()
	if status then
		print('Operations completed successfully.')
	else
		if err then
			print('Failure occurred: ' .. err)
		else
			print("Failed!")
		end
	end
end
