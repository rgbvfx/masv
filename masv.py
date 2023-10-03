from datetime import datetime
import json
import math
import os
import re
import requests
from xml.etree import ElementTree
import masv_env_setup


class MasvController():
    '''
    Class to perform most masv operations.
    '''
    def __init__(self):
        self._account_email = os.environ['MASV_ACCOUNT_EMAIL']
        self._account_password = os.environ['MASV_ACCOUNT_PASSWORD']
        self._api_key = os.environ['MASV_API_KEY']
        self._api_key_id = os.environ['MASV_API_KEY_ID']
        self._chunk_size = 104857600
        self._credentials_path = os.environ['MASV_CRED_PATH']
        self._debug = True
        self._user_token = os.environ['MASV_USER']
        self._team_id = os.environ['MASV_TEAM_ID']


    def get_user_token(self):
        '''
        Send email and password to get user token.
        '''
        headers = {'Content-Type': 'application/json'}
        json_data = {'email': self._account_email, 'password': self._account_password}
        response = requests.post('https://api.massive.app/v1/auth', headers=headers, json=json_data)
        
        save_file_path = os.path.join(self._credentials_path, 'MASV-USER_TOKEN.json')
        with open(save_file_path, "w") as outfile:
            outfile.write(json.dumps(response.json()))
        
        print(response)
        print(f'Saved user token to: {save_file_path}')
        print('Update your environ variables !!!')
        return response


    def calculate_chunks(self, file_size):
        '''
        file_size is in bytes.
        This gets divided into one hundred megabyte chunks.
        Then we count the number of chunks the file will be split into.
        '''
        number_of_chunks = file_size/self._chunk_size
        overage, chunks = math.modf(number_of_chunks)
        if overage > 0:
            chunks += 1
        return int(chunks)


    def read_chunk(self, file_object):
        '''
        generator
        return file chunks
        '''
        def gen(file_object):
            yield file_object.read(self._chunk_size)
        return next(gen(file_object))


    def get_api_key(self):
        '''
        Send the user token and team id to create an api key for the team.
        '''
        headers = {'X-User-Token': self._user_token, 'Content-Type': 'application/json'}
        data = {'name': 'key2', 'expiry': '2050-01-01T00:00:00.000Z', 'state': 'active'}
        response = requests.post(f'https://api.massive.app/v1/teams/{self._team_id}/api_keys', headers=headers, json=data)
        json_object = json.dumps(response.json(), indent=4)

        save_file_path = os.path.join(self._credentials_path, 'MASV-API-KEY.json')
    
        with open(save_file_path, "w") as outfile:
            outfile.write(json_object)

        print(json_object)
        print(f'Saved api key to {save_file_path}')
        print('Update your environ variables !!!')

        return json_object
       

    def update_api_key(self, api_key_id):
        '''
        Send the user_token and api_key_id to update the api key.
        '''
        headers = {'X-User-Token': self._user_token, 'Content-Type': 'application/json'}
        data = {'name': 'key2', 'expiry': '2050-01-01T00:00:00.000Z', 'state': 'active'}
        response = requests.put(f'https://api.massive.app/v1/api_keys/{api_key_id}', headers=headers, json=data)
        json_object = json.dumps(response.json(), indent=4)        
        save_key_path = os.path.join(self._credentials_path, 'MASV-API-KEY-UPDATE.json')
        with open(save_key_path, "w") as outfile:
            outfile.write(json_object)
        print(json_object)
        print(f'Saved new key to {save_key_path}')
        print('Update your environ variables !!!')

        return json_object


    def create_package(self, package_name, package_description, recipients):
        # Upload step #1
        # Create a team package
        '''
        Send user_token and team_id to create a package.
        '''
        headers = {'X-User-Token': self._user_token, 'Content-Type': 'application/json'}
        data = {"description": package_description, "name": package_name, "recipients": recipients}
        response = requests.post(f'https://api.massive.app/v1/teams/{self._team_id}/packages', headers=headers, json=data).json()
        return response


    def add_file_to_package(self, dir_path, file_name, package_token, package_id):
        '''
        Add a file to the package.
        return dict
        '''
        file_path = os.path.join(dir_path, file_name)
        file_stat = os.stat(file_path)
        last_modified = datetime.fromtimestamp(file_stat.st_mtime).isoformat()
        headers = {'X-Package-Token': package_token, 'Content-Type': 'application/json'}
        json_data = {
            'kind': 'file',
            'name': file_name,
            'path': dir_path,
            'last_modified': last_modified
        }
        url = f'https://api.massive.app/v1/packages/{package_id}/files'
        response = requests.post(url, headers=headers, json=json_data).json()
        return response


    def create_file_upload_id(self, blueprint):
        '''
        Get upload id for the file.
        The response is in xml. We have to parse the xml to get the upload_id
        return str
        '''
        # Get bucket and upload id for the file.
        headers = blueprint['headers']
        url = blueprint['url']
        if blueprint['method'] != 'POST':
            msg = f'ERROR: No post method for upload. Instead got method: {blueprint["method"]}'
            print(msg)
            return
    
        response = requests.post(url, headers=headers)
        
        # process the xml response
        tree = ElementTree.fromstring(response.content)

        if not tree:
            msg = 'ERROR: no xml returned'
            print(msg)
            return

        # bucket = None
        # key = None
        upload_id = None
        
        # if tree[0].tag.endswith('Bucket'):
        #    bucket = tree[0].text
            
        # if tree[1].tag.endswith('Key'):
            # key = tree[1].text

        try:
            if tree[2].tag.endswith('UploadId'):
                upload_id = tree[2].text
            if not upload_id:
                msg = 'ERROR: failed to parse UploadID from xml tree tag'
                print(msg)
                print(response.content)
                return
            return upload_id
        except Exception as e:
            msg = 'ERROR: UploadId - xml tree tag error'
            print(msg)
            print(e)


    def get_upload_urls(self, upload_id, package_token, package_id, file_id, number_of_chunks):
        '''
        MASV needs the file to be separated into 100 MB chunks.
        Each chunk needs to be uploaded via it's own url.
        Here we get a list of urls for each chunk we're going to send. 
        '''
        # Obtain upload URLs
        data = {'upload_id': upload_id}
        headers = {'X-Package-Token': package_token, 'Content-Type': 'application/json'}
        url = f'https://api.massive.app/v1/packages/{package_id}/files/{file_id}?start=0&count={number_of_chunks}'
        
        response = requests.post(url, headers=headers, json=data).json()
        return response
    

    def upload_file_parts(self, file_path, upload_urls):
        '''
        Open the file, read each chunk and send to upload url.
        '''
        response_list = []
        headers = {"Content-Type": "application/binary"}

        # open the file
        with open(file_path, 'rb') as file_object:

            # loop through each url
            for url_dict in upload_urls:

                # get the url
                url = url_dict['url']

                # get the part_number
                match = re.findall('(?:&partNumber=)(\d+)(?:&)', url)
                part_number = match[0]
            
                # read the current chunk
                file_chunk = self.read_chunk(file_object)
                try:
                    # send the chunk
                    if self._debug:
                        msg = f'sending chunk part: {part_number}  size: {len(file_chunk)}'
                        print(msg)
                    response = requests.put(url, data=file_chunk, headers=headers)
                    response_list.append((part_number, response))
                except Exception as e:
                    msg = f'ERROR sending chunk part: {part_number}'
                    print(e)
        return response_list


    def finalize_file(self, package_id, package_token, file_id, file_size, upload_id, parts_list):
        '''
        curl -d '{"chunk_extras":[{"partNumber": CHUNK_PART_NUMBER, "etag": CHUNK_ETAG}],
                  "file_extras":{"upload_id": UPLOAD_ID},
                  "size": FILE_SIZE,
                  "chunk_size": CHUNK_SIZE}' \
        -H "X-Package-Token: PACKAGE_TOKEN" \
        -H "Content-Type: application/json" \
        -s -X POST https://api.massive.app/v1/packages/PACKAGE_ID/files/FILE_ID/finalize 
        '''
        chunk_extras_list = []
        for part_tup in parts_list:
            part_number, etag = part_tup
            chunk_extras_list.append({"partNumber": part_number, "etag": etag})
        
        json_data = {"chunk_extras": chunk_extras_list,
                     "file_extras":{"upload_id": upload_id}, 
                     "size": file_size, 
                     "chunk_size": self._chunk_size}
        
        headers = {'X-Package-Token': package_token, 'Content-Type': 'application/json'}

        url = f'https://api.massive.app/v1/packages/{package_id}/files/{file_id}]/finalize'

        return requests.post(url, headers=headers, json=json_data).json()


    def finalize_package(self, package_id, package_token):
        '''
        curl -H "X-Package-Token: $PACKAGE_TOKEN" \
        -H "Content-Type: application/json" \
        -s -X POST https://api.massive.app/v1/packages/PACKAGE_ID/finalize
        '''
        headers = {'X-Package-Token': package_token, 'Content-Type': 'application/json'}
        url = f'https://api.massive.app/v1/packages/{package_id}/finalize'
        return requests.post(url, headers=headers)


class MasvPackageUpload():
    '''
    Class to upload files.
    '''
    def __init__(self, package_name, package_description, file_list, email_recipients):
        self.masv = MasvController()
        self.chunk_size = self.masv._chunk_size
        self.email_recipients = email_recipients
        self.file_list = file_list
        self.file_package = None
        self.package_id = None
        self.package_description = package_description
        self.package_name = package_name
        self.package_token = None

        # internal set up
        self._debug = True
        self.process_file_list()


    def upload_package(self):
        # create the empty package
        package = self.masv.create_package(self.package_name, self.package_description, self.email_recipients)
        self.package_token = package['access_token']
        self.package_id = package['id']
        if self._debug:
            print('created package')
        
        for dir_path, file_name in self.file_list:
            # add file to the package
            file_package = self.masv.add_file_to_package(dir_path, file_name, self.package_token, self.package_id)
            blueprint = file_package['create_blueprint']
            max_chunk_count = file_package['max_chunks_count'] 
            max_chunk_size = file_package['max_chunk_size'] 
            min_chunk_size = file_package['min_chunk_size']
            file_id = file_package['file']['id']
            print('added file to package with file id:', file_id)

            # create file upload id
            upload_id = self.masv.create_file_upload_id(blueprint)
            if self._debug:
                print('upload_id:', upload_id)

            # get the file size
            file_path = os.path.join(dir_path, file_name)
            file_size = os.path.getsize(file_path)

            # get number of file chunks
            number_of_chunks = self.masv.calculate_chunks(file_size)
            if self._debug:
                print('number of file chunks:' , number_of_chunks)

            # get upload urls
            upload_urls = self.masv.get_upload_urls(upload_id, self.package_token, self.package_id, file_id, number_of_chunks)

            # upload file chunks
            response_list = self.masv.upload_file_parts(file_path, upload_urls)

            # process the returned items into a part_list
            part_list = []
            for part_number, response in response_list:
                etag = response.headers['ETag']
                part_list.append((part_number, etag))
                if self._debug:
                    print(f'successfully sent chunk part {part_number}, with etag: {etag}')

            # finalize file
            if self._debug:
                print()
                print('FINALIZING FILE')
            response = self.masv.finalize_file(self.package_id, self.package_token, file_id, file_size, upload_id, part_list)
            if self._debug:
                print(response)

        # finalize package
        print()
        print('FINALIZING PACKAGE')
        response = self.masv.finalize_package(self.package_id, self.package_token)
        print(response)
        

    def process_file_list(self):
        '''
        The file_list can either be a list of absolute file paths,
        or a list of tuples consistening of separate file_path and file_name.
        If necessary, split each absolute file path into file_path and file_name
        and reformat file list.
        '''
        if not self.file_list:
            return

        # if this is a list of absolute file paths
        new_file_list = []
        if type(self.file_list[0]) is str :
            for file_item in self.file_list:
                if os.path.isfile(file_item):
                    file_path = os.path.dirname(file_item)
                    file_name = os.path.basename(file_item)
                    new_file_list.append((file_path, file_name))
                else:
                    msg = f'ERROR: not a valid file: {file_item}'
                    print(msg)
            self.file_list = new_file_list

        # if this is a list of tuples
        if type(self.file_list[0]) is tuple:
            # check the tuple
            directory_path = self.file_list[0][0].replace('\\', '/')
            # if this is a directory the list is already formatted corectly
            if os.path.isdir(directory_path):
                pass
            else:
                msg = f'ERROR: not a valid directory: {directory_path.replace("/", os.path.sep)}'
                print(msg)





# ------------------------------------------------


if __name__ == '__main__':
    # setup
    #dir_path = 'U:\\craig\\ImageItems\\ClickArt'
    #file_name = 'NYBN_026.JPG'
    dir_path = 'V:\\Misc'
    file_name = 'movie_montage_2_6-1.mkv'
    file_path = os.path.join(dir_path, file_name)
    email_list = ['rgbvfx@gmail.com']
    package_name = 'test12'
    package_description = 'test12'
    
    if os.path.isfile(file_path):
        # create instance
        masv_package_upload = MasvPackageUpload(package_name, package_description, [file_path], email_list)
        # execute
        masv_package_upload.upload_package()

    else:
        print('no file')
