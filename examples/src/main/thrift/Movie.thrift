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
  3 	: optional string  release_date
  4 	: optional string  video_release_date
  5 	: optional string  imdb_url
  6 	: bool     isUnknown
  7 	: bool     isAction
  8 	: bool     isAdventure
  9 	: bool     isAnimation
  10 	: bool     isChildrens
  11 	: bool     isComedy
  12 	: bool     isCrime
  13 	: bool     isDocumentary
  14 	: bool     isDrama
  15 	: bool     isFantasy
  16 	: bool     isFilmnoir
  17 	: bool     isHorror
  18 	: bool     isMusical
  19 	: bool     isMystery
  20 	: bool     isRomance
  21 	: bool     isScifi
  22 	: bool     isThriller
  23 	: bool     isWar
  24 	: bool     isWestern
 }