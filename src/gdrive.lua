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

local print = print
local t2spp = t2spp

-- Create the module table here
local M = {}
package.loaded[...] = M
_ENV = M		-- Lua 5.2+

_VERSION = "1.15.12.07"

local baseConfig = {
	auth_url = 'https://accounts.google.com/o/oauth2/auth',
	token_url = 'https://accounts.google.com/o/oauth2/token',
	scope = 'https://www.googleapis.com/auth/drive',
	endpoint = 'https://www.googleapis.com/drive/v2/',
	endpoint_upload = 'https://www.googleapis.com/upload/drive/v2/',
	approval_prompt = 'force',
	curl_options = {ssl_verifypeer = 0},
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
local objItems = {
	'id',		-- Required (code depends on it)
	'mimeType',	-- Required (code depends on it)
	'title',	
	-- Other items stored:
--	gdrive,		-- Google drive connection object (code depends on it)
-- 	path		-- Path of the item
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
	local content, code = self.oauth2:request(url, payload, headers, verb, options)
	if code ~= 200 then 
		--print(content)
		return nil,formatHttpCodeError(code)
	end
	return json.decode(content)
end

-- Returns the parsed URL in a table
local function buildUrl(self, params, endpoint)
	endpoint = endpoint or (self.config.endpoint .. 'files')
	local result = url.parse(endpoint)
	result.query.alt = 'json'
	copyTable(params, result.query)
	return result
end

local function get(self,params, fileId)
	local url = buildUrl(self,params, self.config.endpoint .. 'files/' .. fileId)
	return request(self,url)
end

local function list(self,params,endpoint)
	local url = buildUrl(self,params,endpoint)		-- Returns the parsed URL in a table
	return request(self,url)
end

local objMeta, createObject

do
	local nextListPage
	function nextListPage(dirList)
		if not dirList.nextPageToken or not dirList.items or not dirList.items[1] or not objData[dirList.items[1]] or not dirList.parentID then
			return nil
		end
		local self = objData[dirList.items[1]].gdrive
		local stat,msg = list(self,{pageToken = dirList.nextPageToken, maxResults = dirList.num,q="'"..dirList.parentID.."' in parents"})
		if not stat then 
			return nil,"Cannot get folder listing: "..msg
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
					return
				end
				local self = objData[t].gdrive
				local body = {[prop] = val}
				local url = buildUrl(self,{},self.config.endpoint.."files/"..objData[t].id)		-- Returns the parsed URL in a table
				local resp,msg = request(self, url, json.encode(body),{'Content-Type: application/json'},"PUT")		
				if resp then
					return true
				else
					return nil,msg
				end
			end,
			list = function(t,num)	-- num is number of results in the page
				if not objData[t] or objData[t].mimeType ~= mimeType.folder or not objData[t].gdrive then
					return nil
				end
				num = num or 100
				local self = objData[t].gdrive
				local stat,msg = list(self,{maxResults=num,q="'"..objData[t].id.."' in parents"})
				if not stat then 
					return nil,"Cannot get folder listing: "..msg
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
			mkdir = function(t,dirName)
			end,
			upload = function(t,source)
			end,
			-- Function to move an item from current parents to the given parent object
			move = function(t,dest)		
				if not objData[dest] then
					return nil,"Invalid destination object."
				end
			end,
			rename = function(t,newName)
			end,
			copyto = function(t,dest)
			end,
			delete = function(t)
			end,
			download = function(t,sink)
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
	objData[obj] = t
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

local function insert(self, params, file)
	local url = buildUrl(self,params)		-- Returns the parsed URL in a table
	return request(self, url, json.encode(file), {'Content-Type: application/json'})
end

local function download(self,params, fileId)
	local url = buildUrl(self,params, self.config.endpoint .. 'files/' .. fileId)
	local data = request(self,url)
	local content, code = self.oauth2:request(data.downloadUrl)
	if code ~= 200 then 
		return nil,"Could not fetch data: "..formatHttpCodeError(code)
	end
	return content, data
end

local function upload(self,params, file, blob)
	local url = buildUrl(self,params, self.config.endpoint_upload .. 'files')
	url.query.uploadType = 'multipart'
	local data = {
		{data = json.encode(file), type = 'application/json'},
		{data = blob, type = file.mimeType},
	}
	local content, contentType = buildMultipartRelated(data)
	return request(self, url, content, {'Content-Type: ' .. contentType})
end

local function delete(self,params, fileId)
	local url = buildUrl(self,params, self.config.endpoint .. 'files/' .. fileId)
	local _, code = self.oauth2:request(url, nil, nil, 'DELETE')
	if code ~= 204 then 
		return nil,"Error deleteing: "..formatHttpCodeError(code)
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
	local itemData
	local stat,msg,ret
	-- Now iterate through the path
	for level in namePre:gmatch("%/([^%/]+)") do
		-- Get the parent directory list (children.list API)
		stat,msg = list(self,{},self.config.endpoint.."files/"..parentID.."/children")
		if not stat then
			return nil,"Cannot get directory listing: "..msg
		end
		-- Now loop through each item to search for the level folder
		local found 
		for i = 1,#stat.items do
			-- Get the file information
			ret,msg = get(self,{q = "title = '"..level.."' and mimeType = '"..self.mimeType.folder.."'"},stat.items[i].id)
			if not ret then 
				return nil,"Cannot get file information: "..msg
			end
			if ret.mimeType == self.mimeType.folder then
				-- Found the folder
				found = i
				break
			end			
		end		-- for i = 1,#stat.items do ends here
		if not found then
			return false	-- Since this level folder does not exist, item cannot exist
		end
		parentID = stat.items[found].id
	end
	
	-- Now get the last directory listing where the item is supposed to be
	stat,msg = list(self,{},self.config.endpoint.."files/"..parentID.."/children")
	if not stat then
		return nil,"Cannot get directory listing: "..msg
	end
	-- Now loop through each item to search for the item
	local found 
	for i = 1,#stat.items do
		-- Get the file information
		ret,msg = get(self,{q = "title = '"..itm.."'"},stat.items[i].id)
		if not ret then 
			return nil,"Cannot get file information: "..msg
		end
		--print(ret.title,ret.mimeType)
		print(ret.nextPageToken)
		if (typ == "folder" and ret.mimeType == self.mimeType.folder) or (typ == "file" and ret.mimeType ~= self.mimeType.folder) then
			-- Found the folder
			found = i
			itemData = ret
			break
		end			
	end		-- for i = 1,#stat.items do ends here
	if not found then
		return false	-- Since this level folder does not exist, item cannot exist
	end
	return createObject(self,itemData,namePre.."/")
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
	local itemData
	local stat,msg,ret
	-- Now iterate through the path
	for level in dir:gmatch("%/([^%/]+)") do
		-- Get the parent directory list (children.list API) with the query parameter of title matching level
		stat,msg = list(self,{q = "title = '"..level.."' and mimeType = '"..self.mimeType.folder.."'"},self.config.endpoint.."files/"..parentID.."/children")
		print(stat)
		if not stat then
			return nil,"Cannot get directory listing: "..msg
		end
		-- Now loop through each item to search for the level folder
		local found 
		for i = 1,#stat.items do
			-- Get the file information
			ret,msg = get(self,{},stat.items[i].id)
			if not ret then 
				return nil,"Cannot get file information: "..msg
			end
			if ret.title == level and ret.mimeType == self.mimeType.folder then
				-- Found the folder
				found = i
				itemData = ret
				break
			end			
		end		-- for i = 1,#stat.items do ends here
		if not found then
			-- Create the folder here
			local file = {title = level, mimeType = self.mimeType.folder,parents={{id=parentID}}}
			local url = buildUrl(self,{})		-- Returns the parsed URL in a table
			local folder,msg = request(self, url, json.encode(file), {'Content-Type: application/json'})	
			if not folder then
				return nil,"Could not create folder: "..msg
			end
			itemData = folder
			parentID = folder.id
		else
			parentID = stat.items[found].id
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
		get = get
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
		obj.acquireToken = stat	-- The application using this module will have to aquire the token and call the included function to add the token
	end
	stat,msg = get(obj,{},"root")
	if not stat then
		return nil,"Cannot get the root directory information: "..msg
	end
	obj.root = createObject(obj,stat,"")
	objData[obj.root].title = ""	-- Make the title an empty string so that it does not get added to the paths of subsequent levels
 	return obj
end

