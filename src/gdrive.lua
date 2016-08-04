--[[
The MIT License (MIT)

Copyright (c) 2016 Milind Gupta

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
]]

-- Google Drive access module for Lua 5.2+
local url = require 'net.url'
local json = require 'json'
local oauth2 = require 'oauth2'

local table = table
local math = math
local type = type
local pairs = pairs
local setmetatable = setmetatable
local string = string
local getmetatable = getmetatable
local os = os
local pcall = pcall
local tostring = tostring

--local print = print
--local t2spp = t2spp

-- Create the module table here
local M = {}
package.loaded[...] = M
_ENV = M		-- Lua 5.2+

_VERSION = "1.16.06.16"

local baseConfig = {
	auth_url = 'https://accounts.google.com/o/oauth2/auth',
	token_url = 'https://accounts.google.com/o/oauth2/token',
	scope = 'https://www.googleapis.com/auth/drive',
	endpoint = 'https://www.googleapis.com/drive/v2/',
	endpoint_upload = 'https://www.googleapis.com/upload/drive/v2/',
	approval_prompt = 'force',
	access_type = 'offline',
}

local mimeType = {
	--file = "text/plain",
	audio = "application/vnd.google-apps.audio",
	document = "application/vnd.google-apps.document",
	drawing = "application/vnd.google-apps.drawing",
	file = "application/vnd.google-apps.file",
	folder = 'application/vnd.google-apps.folder',
	form = "application/vnd.google-apps.form",
	fusiontable = "application/vnd.google-apps.fusiontable",
	photo = "application/vnd.google-apps.photo",
	presentation = "application/vnd.google-apps.presentation",
	script = "application/vnd.google-apps.script",
	sites = "application/vnd.google-apps.sites",
	spreadsheet = "application/vnd.google-apps.spreadsheet",
	unknown = "application/vnd.google-apps.unknown",
	video = "application/vnd.google-apps.video"
}

local identifier = {}		
local objData = {}		-- This stores the data tables of the item objects created i.e. directory objects or file objects
local objDataMeta = {__mode = "k"}	-- To make the objData table keys weak
setmetatable(objData,objDataMeta)
local idToObj = {}		-- Table to store the mapping from the item id to the object to reuse objects already created
local idToObjMeta = {__mode = "v"}
setmetatable(idToObj,idToObjMeta)

-- Table to specify what things to store for an item in the data table
-- Here is the full meta data associated with a file (https://developers.google.com/drive/v2/reference/files)
--[[{
  "kind": "drive#file",
  "id": string,
  "etag": etag,
  "selfLink": string,
  "webContentLink": string,
  "webViewLink": string,
  "alternateLink": string,
  "embedLink": string,
  "openWithLinks": {
    (key): string
  },
  "defaultOpenWithLink": string,
  "iconLink": string,
  "thumbnailLink": string,
  "thumbnail": {
    "image": bytes,
    "mimeType": string
  },
  "title": string,
  "mimeType": string,
  "description": string,
  "labels": {
    "starred": boolean,
    "hidden": boolean,
    "trashed": boolean,
    "restricted": boolean,
    "viewed": boolean
  },
  "createdDate": datetime,
  "modifiedDate": datetime,
  "modifiedByMeDate": datetime,
  "lastViewedByMeDate": datetime,
  "markedViewedByMeDate": datetime,
  "sharedWithMeDate": datetime,
  "version": long,
  "sharingUser": {
    "kind": "drive#user",
    "displayName": string,
    "picture": {
      "url": string
    },
    "isAuthenticatedUser": boolean,
    "permissionId": string,
    "emailAddress": string
  },
  "parents": [
    parents Resource
  ],
  "downloadUrl": string,
  "downloadUrl": string,
  "exportLinks": {
    (key): string
  },
  "indexableText": {
    "text": string
  },
  "userPermission": permissions Resource,
  "permissions": [
    permissions Resource
  ],
  "originalFilename": string,
  "fileExtension": string,
  "fullFileExtension": string,
  "md5Checksum": string,
  "fileSize": long,
  "quotaBytesUsed": long,
  "ownerNames": [
    string
  ],
  "owners": [
    {
      "kind": "drive#user",
      "displayName": string,
      "picture": {
        "url": string
      },
      "isAuthenticatedUser": boolean,
      "permissionId": string,
      "emailAddress": string
    }
  ],
  "lastModifyingUserName": string,
  "lastModifyingUser": {
    "kind": "drive#user",
    "displayName": string,
    "picture": {
      "url": string
    },
    "isAuthenticatedUser": boolean,
    "permissionId": string,
    "emailAddress": string
  },
  "ownedByMe": boolean,
  "editable": boolean,
  "canComment": boolean,
  "shareable": boolean,
  "copyable": boolean,
  "writersCanShare": boolean,
  "shared": boolean,
  "explicitlyTrashed": boolean,
  "appDataContents": boolean,
  "headRevisionId": string,
  "properties": [
    properties Resource
  ],
  "folderColorRgb": string,
  "imageMediaMetadata": {
    "width": integer,
    "height": integer,
    "rotation": integer,
    "location": {
      "latitude": double,
      "longitude": double,
      "altitude": double
    },
    "date": string,
    "cameraMake": string,
    "cameraModel": string,
    "exposureTime": float,
    "aperture": float,
    "flashUsed": boolean,
    "focalLength": float,
    "isoSpeed": integer,
    "meteringMode": string,
    "sensor": string,
    "exposureMode": string,
    "colorSpace": string,
    "whiteBalance": string,
    "exposureBias": float,
    "maxApertureValue": float,
    "subjectDistance": integer,
    "lens": string
  },
  "videoMediaMetadata": {
    "width": integer,
    "height": integer,
    "durationMillis": long
  },
  "spaces": [
    string
  ]
}]]
-- objItems is used as a template to decide which properties to store of the object in the objData table
local objItems = {
	'id',		-- Required (code depends on it)
	'mimeType',	-- Required (code depends on it)
	'title',	-- Required (code depends on it)
	-- Other items stored:
--	gdrive,		-- Google drive connection object (code depends on it)
-- 	path		-- Path of the item (code depends on it)
}

local function copyTable(source, target)
	for k, v in pairs(source) do
		target[k] = v
	end
end

local function formatHttpCodeError(x)
	return string.format('Bad http response code: %d.', x)
end

local function request(self, url, payload, headers, verb, options)
	local content, code, tokenUpdated = self.oauth2:request(url, payload, headers, verb, options)
	self.tokenUpdated = self.tokenUpdated or tokenUpdated
	if code < 200 or code > 206 then 
		--print(content)
		return nil,formatHttpCodeError(code),content
	end
	return json.decode(content) or ""
end

-- Returns the parsed URL in a table
local function buildUrl(self, params, endpoint)
	endpoint = endpoint or (self.config.endpoint .. 'files')
	local result = url.parse(endpoint)
	result.query.alt = 'json'
	copyTable(params, result.query)
	return result
end

-- Function to get information for a file given the fileId
local function get(self,params, fileId)
	local url = buildUrl(self,params, self.config.endpoint .. 'files/' .. fileId)
	return request(self,url)
end

-- Function to get data from an endpoint
local function list(self, params, endpoint)
	local url = buildUrl(self,params,endpoint)		-- Returns the parsed URL in a table
	return request(self,url)
end

-- Function to send a body to an endpoint
-- verb is the http method like "GET", "POST", "PUT". Since there is a body the default verb is "POST"
local function insert(self, params, body, endpoint, verb)
	local url = buildUrl(self,params,endpoint)		-- Returns the parsed URL in a table
	return request(self, url, json.encode(body), {["Content-Type"] = "application/json"}, verb)
end

local objMeta, createObject

do
	local nextListPage
	local function generateBoundary()
		math.randomseed(os.time())
		local rnd = function() return string.sub(math.random(), 3) end
		local result = {}
		for i = 1,5 do table.insert(result, rnd()) end
		return table.concat(result)
	end

	local function buildMultipartRelated(parts)
		local boundary = generateBoundary()
		local result = {}
		for _,part in pairs(parts) do
			-- delimiter
			table.insert(result, '--' .. boundary)
			-- encapsulation
			table.insert(result, '\r\n')
			table.insert(result, 'Content-Type: ' .. part.type .. '\r\n')
			table.insert(result, '\r\n')
			table.insert(result, part.data)
			table.insert(result, '\r\n')
		end
		-- close-delimiter
		table.insert(result, '--' .. boundary .. '--' .. '\r\n')
		return table.concat(result), 'multipart/related; boundary=' .. boundary
	end
	
	function nextListPage(dirList)
		if not dirList.nextPageToken or not dirList.items or not dirList.items[1] or not objData[dirList.items[1]] or not dirList.parentID then
			return nil, "Invalid directory listing object."
		end
		local self = objData[dirList.items[1]].gdrive
		local stat,msg,msg2 = list(self,{pageToken = dirList.nextPageToken, maxResults = dirList.num,q="'"..dirList.parentID.."' in parents"})
		if not stat then 
			return nil,"Cannot get folder listing: "..msg,msg2
		end
		-- Create the directory listing object
		local nextList = {
			next = nextListPage,
			items = {},
			parentID = dirList.parentID,
			nextPageToken = stat.nextPageToken,
			num = dirList.num
		}
		for i = 1,#stat.items do
			nextList.items[#nextList.items + 1] = createObject(self,stat.items[i],objData[dirList.items[1]].path)
		end
		return nextList
	end
	objMeta = {
		__metatable = "Metatable locked",
		__index = {
			getProperty = function(t,prop)	
				if not objData[t] then
					return nil	-- Not a valid object
				end
				if objData[t][prop] then
					return objData[t][prop]
				end
				local self = objData[t].gdrive
				local ret = get(self,{},objData[t].id)
				if not ret then 
					return nil
				end
				return ret[prop]
			end,
			setProperty = function(t,prop,val)
				if not objData[t] then
					return nil, "Invalid item object"
				end
				local self = objData[t].gdrive
				local resp,msg = insert(self,{},{[prop] = val},self.config.endpoint.."files/"..objData[t].id,"PUT")
				if resp then
					if objData[t][prop] then
						objData[t][prop] = val
					end
					return true
				else
					return nil,msg
				end
			end,
			list = function(t,num)	-- num is number of results in the page
				if not objData[t] or objData[t].mimeType ~= mimeType.folder or not objData[t].gdrive then
					return nil,"Object not valid or not a folder"
				end
				num = num or 100
				local self = objData[t].gdrive
				local stat,msg,msg2 = list(self,{maxResults=num,q="'"..objData[t].id.."' in parents"})
				if not stat then 
					return nil,"Cannot get folder listing: "..msg,msg2
				end
				-- Create the directory listing object
				local dirList = {
					next = nextListPage,
					items = {},
					parentID = objData[t].id,
					nextPageToken = stat.nextPageToken,
					num = num
				}
				for i = 1,#stat.items do
					dirList.items[#dirList.items + 1] = createObject(objData[t].gdrive,stat.items[i],objData[t].path..objData[t].title.."/")
				end
				return dirList
			end,
			-- function to check whether an item exists
			-- name is the name of the item in this folder
			-- typ is the type of item. Valid values are 'folder' or 'file'. Default is 'file'
			-- Returns nil, Error message in case of error or failure
			-- Returns item object if it exits
			-- Returns false if it does not exist 
			item = function(t,name,typ)
				if not objData[t] or objData[t].mimeType ~= mimeType.folder then
					return nil,"Object not valid or not a folder"
				end
				if not name or type(name) ~= "string" then
					return nil, "Need a valid string name"
				end
				typ = typ or "file"
				if type(typ) ~= "string" or (typ:lower() ~= "folder" and typ:lower() ~= "file") then
					return nil,"Type argument should be either 'folder' or 'file'"
				end
				typ = typ:lower()
				local self = objData[t].gdrive
				local parentID = objData[t].id
				local compareSign
				if typ == "folder" then
					compareSign = "="
				else
					compareSign = "!="
				end
				local stat,msg = list(self,{q="'"..parentID.."' in parents and title = '"..name.."' and mimeType "..compareSign.." '"..self.mimeType.folder.."'"})
				if not stat then
					return nil,"Cannot get directory listing: "..msg
				end
				if not stat.items[1] then
					return false	-- No item found
				end
				return createObject(self,stat.items[1],objData[t].path..objData[t].title.."/")
			end,
			-- Function to create a directory with the given name, the function does not allow slashes in the name
			mkdir = function(t,dirName)
				if not objData[t] or objData[t].mimeType ~= mimeType.folder or not objData[t].gdrive then
					return nil,"Object not valid or not a folder"
				end
				if not dirName or type(dirName) ~= "string" or dirName:find("/") or dirName:find([[\]]) then
					return nil,"Invalid directory name"
				end
				local self = objData[t].gdrive
				-- Check if it already exists
				local folder = t:item(dirName,"folder")
				if folder then
					-- It already exists
					return folder
				end
				local msg
				-- Create the folder here
				folder, msg = insert(self,{},{title = dirName, mimeType = self.mimeType.folder,parents={{id=objData[t].id}}})
				if not folder then
					return nil,"Could not create folder: "..msg
				end
				return createObject(self,folder,objData[t].path..objData[t].title..[[/]])
			end,
			-- Function to upload file to google drive. Right now only uploads using uploadType = multipart, have to work on making 
			-- it automatically choose between multipart and resumable
			-- title is the name of the file
			-- source is a function which on each call returns the next chunk of data
			-- It will not upload if the file already exists unless force is true
			upload = function(t,title,source,force)
				if not objData[t] or objData[t].mimeType ~= mimeType.folder or not objData[t].gdrive then
					return nil,"Object not valid or not a folder"
				end
				if not source or type(source) ~= "function" then
					return nil,"Need a data source function."
				end
				if not title or type(title) ~= "string" or title:find("/") or title:find([[/]]) then
					return nil,"Invalid file name"
				end
				-- Check if it already exists
				local file = t:item(title)
				if file then
					-- It already exists
					if not force then
						return nil, "File already exists."
					else
						-- Delete the file here
						local resp,msg = file:delete()
						if not resp then
							return nil,"Unable to delete existing file: "..msg
						end
					end
				end
				
				-- Upload the file here
				local blobTab = {}
				local stat,chunk = pcall(source)
				if not stat then
					return nil, "Error calling the data source function: "..chunk
				end
				while chunk do
					blobTab[#blobTab + 1] = chunk
					stat,chunk = pcall(source)
					if not stat then
						return nil, "Error calling the data source function: "..chunk
					end
				end
				local blob = table.concat(blobTab)
				local self = objData[t].gdrive				
				local body = {title = title, mimeType = "text/plain", parents = {{id = objData[t].id}}}
				local data = {
					{data = json.encode(body), type = 'application/json'},
					{data = blob, type = "text/plain"},
				}
				local content, contentType = buildMultipartRelated(data)
				local url = buildUrl(self,{uploadType = 'multipart'}, self.config.endpoint_upload .. 'files')
				local item,msg = request(self, url, content, {["Content-Type"] = contentType})
				if not item then
					return nil, "Error uploading file: "..msg
				end
				return createObject(self,item,objData[t].path..objData[t].title.."/")
			end,
			-- Function to move an item from current parents to the given parent object
			-- It will not move the item if a same name and type of item already exists at the destination unless force is true
			move = function(t,dest,force)		
				if not objData[dest] or objData[dest].mimeType ~= mimeType.folder then
					return nil,"Invalid destination object."
				end
				if not objData[t] then
					return nil, "Invalid item object"
				end
				local self = objData[t].gdrive
				if objData[t].path == objData[dest].path..objData[dest].title.."/" then
					return nil,"Source and destination paths the same."
				end	
				
				-- Check if it already exists
				local typ 
				if objData[t].mimeType == mimeType.folder then
					typ = "folder"
				end
				local item = dest:item(objData[t].title,typ)
				if item then
					-- It already exists
					if not force then
						return nil, "Similar item already exists at destination."
					else
						-- Delete the item here
						local resp,msg = item:delete()
						if not resp then
							return nil,"Unable to delete existing item: "..msg
						end
					end
				end
				
				-- Get the current parent id
				local parents = t:getProperty("parents")
				if not parents then
					return nil,"Cannot retrieve the current parents for the item object"
				end
				local currParents = ""
				for i = 1,#parents do
					currParents = currParents..parents[i].id..","
				end
				-- Remove the last comma
				currParents = currParents:sub(1,-2)
				local resp,msg = insert(self,{addParents = objData[dest].id,removeParents = currParents},{},self.config.endpoint.."files/"..objData[t].id,"PUT")
				if resp then
					-- Change the path of the object here
					objData[t].path = objData[dest].path..objData[dest].title.."/"
					return true
				else
					return nil,msg
				end
			end,
			-- Function to rename an item to a new name
			rename = function(t,newName,force)
				if not objData[t] then
					return nil, "Invalid item object"
				end
				if not newName or type(newName) ~= "string" or newName:find("/") or newName:find([[\]]) then
					return nil,"Invalid directory name"
				end
				if newName == objData[t].title then	-- name is same as old name
					return true
				end
				-- Check if it already exists
				local typ 
				if objData[t].mimeType == mimeType.folder then
					typ = "folder"
				end
				local self = objData[t].gdrive
				local item = self:item(objData[t].path..newName,typ)	-- Cannot use t:item since t may be a file
				if item then
					-- It already exists
					if not force then
						return nil, "Item with the same name already exists."
					else
						-- Delete the item here
						local resp,msg = item:delete()
						if not resp then
							return nil,"Unable to delete existing item: "..msg
						end
					end
				end
				return t:setProperty("title",newName)
			end,
			
			copyto = function(t,dest,force)
				if not objData[dest] or objData[dest].mimeType ~= mimeType.folder then
					return nil,"Invalid destination object."
				end
				if not objData[t] or objData[t].mimeType == mimeType.folder then
					return nil, "Invalid item object or item a folder"
				end
				if objData[t].path == objData[dest].path..objData[dest].title.."/" then
					return nil,"Source and destination paths the same."
				end	
				-- Check if it already exists
				local typ 
				if objData[t].mimeType == mimeType.folder then
					typ = "folder"
				end
				local item = dest:item(objData[t].title,typ)
				if item then
					-- It already exists
					if not force then
						return nil, "Item with the same name already exists."
					else
						-- Delete the item here
						local resp,msg = item:delete()
						if not resp then
							return nil,"Unable to delete existing item: "..msg
						end
					end
				end
				local self = objData[t].gdrive
				local resp,msg = insert(self,{},{parents = {{id = objData[dest].id}}},self.config.endpoint.."files/"..objData[t].id.."/copy")
				if resp then
					return createObject(self,resp,objData[dest].path..objData[dest].title.."/")
				else
					return nil,"Error copying: "..msg
				end
			end,
			-- Function to delete an item from the Google Drive
			delete = function(t)
				if not objData[t] then
					return nil, "Invalid item object"
				end
				local self = objData[t].gdrive
				local url = buildUrl(self,{}, self.config.endpoint .. 'files/' .. objData[t].id)
				local resp,msg = request(self,url,nil,nil,"DELETE")
				if not resp then
					return nil,"Error deleting: "..msg
				end
				return true
			end,
			-- Function to download a file or portion of it
			download = function(t,sink,strt,stp)
				if not objData[t] or objData[t].mimeType == mimeType.folder then
					return nil, "Invalid item object"
				end
				if not sink or type(sink) ~= "function" then
					return nil,"Need a data sink function."
				end
				if strt and (type(strt) ~= "number" or strt%1 ~= 0) then
					return nil,"Invalid start byte position."
				end
				if stp and (type(stp) ~= "number" or stp%1 ~= 0) then
					return nil,"Invalid stop byte position."
				end
				if strt and stp and stp < strt then
					return nil,"Stop position should be equal or larger that start position"
				end
				local range
				if strt or stp then
					range = "bytes="
					if strt then
						range = range..tostring(strt)
					end
					range = range.."-"
					if stp then
						range = range..tostring(stp)
					end
				end
				local self = objData[t].gdrive
				-- Get the download URL
				local data,code = t:getProperty("downloadUrl")
				if not data then
					return nil,"Error getting download URL: "..code
				end
				local tokenUpdated
				data, code, tokenUpdated = self.oauth2:request(data, nil, {Range=range})
				self.tokenUpdated = self.tokenUpdated or tokenUpdated
				if code < 200 or code > 206 then 
					--print(content)
					return nil,"Error downloading: "..formatHttpCodeError(code),data
				end
				return pcall(sink,data)
			end
		},
		__newindex = function(t,k,v)
			-- Do nothing
		end
	}
end

-- Function to create a object for the drive item
-- The object has the properties cached which are listed in objItems
-- The object has the following methods:
-- * getProperty - To get a metadata property of the object. If not cached it will be retrieved from google drive
-- * setProperty - To set a metadata property of the object.
-- * list - (Only for folder) To get a list of items in the folder
-- * 
function createObject(gdrive,itemData,path)
	-- Check if the item object exists
	for k,v in pairs(idToObj) do
		if k == itemData.id then
			-- Found the same object
			return v
		end
	end
	-- Object not found so create a new one
	local obj = {}
	local t = {}
	for i = 1,#objItems do
		t[objItems[i]] = itemData[objItems[i]]
	end
	t.gdrive = gdrive
	t.path = path
	setmetatable(obj,objMeta)
	idToObj[itemData.id] = obj
	objData[obj] = t	-- all cached data stored here for the item object
	return obj
end

local function validateConfig(config)
	if not config.endpoint or type(config.endpoint) ~= "string" then
		return nil,"Configuration does not have a string endpoint URI"
	end
	if config.endpoint_upload and type(config.endpoint_upload) ~= "string" then
		return nil,"Configuration does not have a string endpoint_upload URI"
	end
	return true
end

-- Function to check whether a particular item exists
-- name is the name of the item with the full path of the item from the root
-- typ is the type of item. Valid values are 'folder' or 'file'. Default is 'file'
-- Returns nil, Error message in case of error or failure
-- Returns item object if it exits
-- Returns false if it does not exist 
local function item(self,name,typ)
	if getmetatable(self) ~= identifier then
		return nil, "Invalid gdrive object"
	end
	if not self.oauth2.tokens then
		return nil,"Access token not acquired. Information in amazondrive.acquireToken."
	end
	if not name or type(name) ~= "string" then
		return nil, "Need a valid string directory name"
	end
	typ = typ or "file"
	if type(typ) ~= "string" or (typ:lower() ~= "folder" and typ:lower() ~= "file") then
		return nil,"Type argument should be either 'folder' or 'file'"
	end
	typ = typ:lower()
	-- Replace back slashes with front slashes and remove the last slash
	name = name:gsub([[\]],[[/]]):gsub([[([%/]*)$]],"")
	-- Add a beginning front slash if not there already
	if name:sub(1,1) ~= [[/]] then
		name = [[/]]..name
	end
	-- Separate the path and the last item
	local namePre,itm = name:match("^(.*)%/(.-)$")
	local parentID = "root"
	local stat,msg
	-- Now iterate through the path
	for level in namePre:gmatch("%/([^%/]+)") do
		-- See if this item is there in the parent folder
		stat,msg = list(self,{q="'"..parentID.."' in parents and title = '"..level.."' and mimeType = '"..self.mimeType.folder.."'"})
		if not stat then
			return nil,"Cannot get directory listing: "..msg
		end
		if not stat.items[1] then
			return false	-- Since this level folder does not exist, item cannot exist
		end
		parentID = stat.items[1].id
	end
	
	-- Now check the last directory for the item 
	local compareSign
	if typ == "folder" then
		compareSign = "="
	else
		compareSign = "!="
	end
	stat,msg = list(self,{q="'"..parentID.."' in parents and title = '"..itm.."' and mimeType "..compareSign.." '"..self.mimeType.folder.."'"})
	if not stat then
		return nil,"Cannot get directory listing: "..msg
	end
	if not stat.items[1] then
		return false	-- Since this level folder does not exist, item cannot exist
	end
	return createObject(self,stat.items[1],namePre.."/")
end

-- Function to make a directory given the string directory name
-- dir is a name of the directory like 'a/b/c' or '/a/b/c' (both mean the same and start from the root). Back slashes can be used in place of forward slashes
-- NOTE: Google drive accepts all ASCII characters for names
-- The function will create the fill path given in the directory structure if not there already
-- Returns nil, Error message in case of error or failure
-- Returns directory object of the new directory created
local function mkdir(self,dir)
	if getmetatable(self) ~= identifier then
		return nil, "Invalid gdrive object"
	end
	if not self.oauth2.tokens then
		return nil,"Access token not acquired. Information in gdrive.acquireToken."
	end
	if not dir or type(dir) ~= "string" then
		return nil, "Need a valid string directory name"
	end
	-- Replace back slashes with front slashes and remove the last slash
	dir = dir:gsub([[\]],[[/]]):gsub([[([%/]*)$]],"")
	-- Add a beginning front slash if not there already
	if dir:sub(1,1) ~= [[/]] then
		dir = [[/]]..dir
	end
	local parentID = "root"
	local stat,msg, itemData
	-- Now iterate through the path
	for level in dir:gmatch("%/([^%/]+)") do
		-- Check whether level folder exists under this parent
		stat,msg = list(self,{q="'"..parentID.."' in parents and title = '"..level.."' and mimeType = '"..self.mimeType.folder.."'"})
		if not stat then
			return nil,"Cannot get directory listing: "..msg
		end
		if not stat.items[1] then
			-- Create the folder here
			local folder, msg = insert(self,{},{title = level, mimeType = self.mimeType.folder,parents={{id=parentID}}})
			if not folder then
				return nil,"Could not create folder: "..msg
			end
			itemData = folder
			parentID = folder.id
		else
			itemData = stat.items[1]
			parentID = stat.items[1].id
		end
	end		-- for level in dir:gmatch("%/([^%/]+)") do ends
	return createObject(self,itemData,dir:match("^(.*%/).-$"))
end

-- Function to create a new connection object for Google drive
-- config is a table containing the parameters for the google drive connection
function new(config)
	local stat,msg,msg2
	local obj = {
		config = {},
		mimeType = mimeType,
		mkdir = mkdir,
		item = item,
		tokenUpdated = false,
		root = nil,
		oauth2 = nil,
		acquireToken = nil
	}
	setmetatable(obj,identifier)	-- To validate teh Google Drive connection object
	-- Create the object configuration
	copyTable(baseConfig, obj.config)
	copyTable(config, obj.config)

	stat,msg = validateConfig(obj.config)
	if not stat then
		return nil,"Invalid Configuration: "..msg
	end

	obj.oauth2,msg = oauth2.new(obj.config)
	if not obj.oauth2 then
		return nil,"Cannot create the OAuth2 connection object: "..msg
	end
	if not obj.oauth2.tokens then
		stat = obj.oauth2:acquireToken()
		-- The application using this module will have to acquire the token and call the included function to add the token
		obj.acquireToken = {stat[1], function(code)		-- Wrapper to the function passed by the oauth2 module to get the root element as well
			local ret,msg,content = stat[2](code)
			if not ret then
				return nil,msg,content
			end
			ret,msg = get(obj,{},"root")
			if not ret then
				return nil,"Cannot get the root directory information: "..msg
			end
			obj.root = createObject(obj,ret,"")
			objData[obj.root].title = ""	-- Make the title an empty string so that it does not get added to the paths of subsequent levels			
			return true
		end}
	else
		-- Tokens are already there so place the acquireToken result in acquireToken and get the root listing
		obj.acquireToken = obj.oauth2:acquireToken()
		stat,msg = get(obj,{},"root")
		if not stat then
			return nil,"Cannot get the root directory information: "..msg
		end
		obj.root = createObject(obj,stat,"")
		objData[obj.root].title = ""	-- Make the title an empty string so that it does not get added to the paths of subsequent levels
	end
 	return obj
end

