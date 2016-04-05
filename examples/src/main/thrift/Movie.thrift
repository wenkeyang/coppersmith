#   Copyright 2014 Commonwealth Bank of Australia
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

#@namespace scala commbank.coppersmith.examples.thrift

struct Movie {
  1 	: string  id
  2 	: string  title
  3 	: string  release_date
  4 	: optional string  video_release_date
  5 	: optional string  imdb_url
  6 	: i32     unknown
  7 	: i32     action
  8 	: i32     adventure
  9 	: i32     animation
  10 	: i32     childrens
  11 	: i32     comedy
  12 	: i32     crime
  13 	: i32     documentary
  14 	: i32     drama
  15 	: i32     fantasy
  16 	: i32     filmnoir
  17 	: i32     horror
  18 	: i32     musical
  19 	: i32     mystery
  20 	: i32     romance
  21 	: i32     scifi
  22 	: i32     thriller
  23 	: i32     war
  24 	: i32     western
 }