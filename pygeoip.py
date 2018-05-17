#!/usr/bin/env python
#coding=utf-8
#encoding:utf8
import sys
import geoip2.database

reload(sys)
sys.setdefaultencoding('utf-8')
type = sys.getfilesystemencoding()

reader = geoip2.database.Reader('E:\GeoLite2-City.mmdb')

response = reader.city("129.250.5.254")

print "\n--- 国家 --------".decode('utf-8').encode(type)
print response.country.iso_code
print response.country.name
print response.country.names['zh-CN']

print "\n--- 省份 --------".decode('utf-8').encode(type)
print response.subdivisions.most_specific.iso_code
print response.subdivisions.most_specific.name
#print response.subdivisions.most_specific.names['zh-CN']

print "\n--- 城市 --------".decode('utf-8').encode(type)
print response.postal.code
print response.city.name
print response.city.names['zh-CN']

print "\n--- 位置（经纬度） --------".decode('utf-8').encode(type)
print response.location.latitude
print response.location.longitude

