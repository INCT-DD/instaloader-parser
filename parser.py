import os
import lzma
import orjson
import requests
import pandas as pd
from glob import glob
from datetime import datetime

path = ''

print('Entering provided directory...')
os.chdir(path)

columns = ['filename', 'datetime', 'type', 'locations_id', 'locations_name', 'mentions', 'hashtags', 'video_duration']

dataframe = pd.DataFrame(columns=[])

print('Traversing file tree...')

for item in os.scandir(path):
    if item.is_dir():
        os.chdir(item.path)
        for file in glob('*UTC.json.xz'):
            with open(file, 'r') as filecontent:
                filename = filecontent.name
                print('Found JSON file: ' + filename + '. Loading...')

                try:
                    unpack_metadata = lzma.open(filename, mode='rt', encoding='utf-8').read()
                    metadata = orjson.loads(unpack_metadata)
                except IOError as e:
                    print("I/O Error. Couldn't load file. Trying the next one...")
                    continue
                else:
                    pass

                hashtags = []
                mentions = []
                locations_id = []
                locations_name = []
                savefile = item.path + '.metadata.csv'

                print('Collecting relevant metadata...')

                datetime = datetime.fromtimestamp(int(metadata['node']['taken_at_timestamp']))
                type_ = metadata['node']['__typename']

                if metadata['node']['is_video'] is True:
                    video_duration = metadata['node']['video_duration']
                else:
                    video_duration = None

                for object_ in metadata['node']['tappable_objects']:
                    if object_['__typename'] == "GraphTappableHashtag":
                        hashtags.append(object_['name'])
                    elif object_['__typename'] == "GraphTappableMention":
                        mentions.append(object_['username'])
                    elif object_['__typename'] == "GraphTappableLocation":
                        locations_id.append(object_['id'])

                minedata = {'filename': filename, 'datetime': datetime, 'type': type_, 'locations_id': locations_id,
                            'locations_name': locations_name, 'mentions': mentions, 'hashtags': hashtags,
                            'video_duration': video_duration}

                print('Writing to dataframe...')

                dataframe = dataframe.append(minedata, ignore_index=True)

                print('Closing file...')

                del metadata
                filecontent.close()

                print('Storing dataframe to CSV file...')

                dataframe.to_csv(savefile)

print('Done.')
