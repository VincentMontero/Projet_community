import requests
from bs4 import BeautifulSoup

url = "https://app.circle.so/api/v1/communities"
headers = {"Authorization": "Token vitu1xgNunssaUDtaCL86KPg"}

def get_html_txt(html):
    soup = BeautifulSoup(html, 'html.parser')
    text_content = soup.get_text(separator='\n', strip=True)
    return text_content

def get_value(json_data, key):
    try:
        if isinstance(json_data, list):
            first_element = json_data[0]
            value = first_element[key]
        elif isinstance(json_data, dict):
            value = json_data[key]
        else:
            print("Invalid JSON data type. Supported types: list or dict.")
            return None

        return value
    except (KeyError, IndexError, TypeError):
        print(f"Key '{key}' not found in the JSON data.")
        return None
    
def get_nested_value(json_data, keys):
    try:
        for key in keys:
            if isinstance(json_data, list):
                first_element = json_data[0]
                json_data = first_element[key]
            elif isinstance(json_data, dict):
                json_data = json_data[key]
            else:
                print("Invalid JSON data type. Supported types: list or dict.")
                return None

        return json_data
    except (KeyError, IndexError, TypeError):
        print(f"Keys '{keys}' not found in the JSON data.")
        return None

def get_all_spaces(community_id):
    space_index = requests.get("https://app.circle.so/api/v1/space_groups?community_id=COM_ID".replace("COM_ID", str(community_id)), headers=headers)
    space_list = []

    if space_index.status_code == 200:
        print(community_id)
        my_json = space_index.json()
        for i in my_json:
            print(f"name: {get_value(i, 'name')}")
            my_id = get_value(i, 'id')
            space_list.append(my_id)

    return space_list

def get_single_space(community_id, space_id: int):
    #132285
    api = "https://app.circle.so/api/v1/space_groups/{SPACE_ID}?community_id={COM_ID}"
    replacements = {"SPACE_ID": str(space_id), "COM_ID": str(community_id)}
    formatted_api = api.format(**replacements)

    space_show = requests.get(formatted_api, headers=headers)

    if space_show.status_code == 200:
        return space_show.json()

def get_all_posts(community_id, space_id):
    api = "https://app.circle.so/api/v1/posts?community_id={COM_ID}&space_id={SPACE_ID}"
    replacements = {"COM_ID": str(community_id), "SPACE_ID": str(space_id)}
    formatted_api = api.format(**replacements)
    
    post_index = requests.get(formatted_api, headers=headers)

    print(f"Trying to get all posts inside room: {space_id} of {community_id}")
    if post_index.status_code == 200:
        my_json = post_index.json()
        for i in my_json:
            print("NEW POST FOUND:")
            print(f"post_id: {get_value(i, 'id')}")
            print(f"nested_body_id: {get_nested_value(i, ['body', 'id'])}")
            print(f"Name: {get_value(i, 'name')}")
            elem = get_nested_value(i, ['body', 'body'])
            if elem:
                elem_text = get_html_txt(elem)
                print(f"{elem_text}")
            print("")
            
def create_new_post(community_id, space_id, name, body):
    api = "https://app.circle.so/api/v1/posts?community_id={COM_ID}&space_id={SPACE_ID}&name={NAME_VALUE}&body={BODY_VALUE}&is_pinned=false&is_comments_enabled=true&is_comments_closed=false&is_liking_enabled=true&skip_notifications=true&slug=check&meta_title=Meta Title&meta_description=Meta Description&opengraph_title=Opengraph Title&opengraph_description=Opengraph Description&hide_from_featured_areas=false"
    replacements = {"COM_ID": str(community_id), "SPACE_ID": str(space_id), "NAME_VALUE": name, "BODY_VALUE": body}
    formatted_api = api.format(**replacements)

    post_create = requests.post(formatted_api, headers=headers)
    if post_create.status_code == 200:
        print("Successfully created new post")
        
def create_comment(community_id, space_id, post_id, body):
    api = "https://app.circle.so/api/v1/comments?community_id={COM_ID}&space_id={SPACE_ID}&post_id={POST_ID}&body={BODY_VALUE}"
    replacements = {"COM_ID": str(community_id), "SPACE_ID": str(space_id), "POST_ID": post_id, "BODY_VALUE": body}
    formatted_api = api.format(**replacements)
    
    post_create = requests.post(formatted_api, headers=headers)
    if post_create.status_code == 200:
        print("Successfully created new comment")

def delete_post(community_id, post_id):
    api = "https://app.circle.so/api/v1/posts/{POST_ID}?community_id={COM_ID}"
    replacements = {"COM_ID": str(community_id), "POST_ID": str(post_id)}
    formatted_api = api.format(**replacements)

    post_delete = requests.delete(formatted_api, headers=headers)
    if post_delete.status_code == 200:
        print("Successfully deleted the post.")
    else:
        print(f"Delete was not successful: {post_delete.status_code}")
        
def get_comment(community_id, space_id):
    api = "https://app.circle.so/api/v1/comments?community_id={COM_ID}&space_id={SPACE_ID}"
    replacements = {"COM_ID": str(community_id), "SPACE_ID": str(space_id)}
    formatted_api = api.format(**replacements)

    index_comment = requests.get(formatted_api, headers=headers)
    if index_comment.status_code == 200:
        my_json = index_comment.json()
        for i in my_json:
            print(f"id: {get_value(i, 'id')}")
            print(f"Content: {get_nested_value(i, ['body', 'body'])}")

def get_all_members(per_page: int, page: int):
    api = "https://app.circle.so/api/v1/community_members?sort=latest&per_page={PER_PAGE_ID}&page={PAGE_ID}"
    replacements = {"PER_PAGE_ID": str(per_page), "PAGE_ID": str(page)}
    formatted_api = api.format(**replacements)
    
    index_members = requests.get(formatted_api, headers=headers)
    if index_members.status_code == 200:
        my_json = index_members.json()
        if not my_json:
            return False
        
        for member in my_json:
            #print(member)
            print((get_value(member, 'id'), get_value(member, 'first_name'),
                   get_value(member, 'last_name'), get_value(member, 'email')))
    return True

if __name__ == '__main__':
    response = requests.get(url, headers=headers)
    community_id = ""
    if response.status_code == 200:
        for element in response.json():
            community_id = get_value(element, "id")

    space_list = get_all_spaces(community_id)
    print(space_list), print('\n')
    space_rooms = get_value(get_single_space(community_id, space_list[1]), "space_order_array") #salons à l'intérieur d'un space
    print(space_rooms), print('\n')
    get_all_posts(community_id, space_rooms[1]) #le com_id est pas check
    get_comment(community_id, space_rooms[1]), print('\n')
    page_number = 1
    while get_all_members(30, page_number):
        page_number += 1
    
    # delete_post(community_id, 9030835) #post_id
    # create_new_post(community_id, space_rooms[1], "test", "vent")
    # create_comment(community_id, space_list[1], 9030844, "Test comment")
