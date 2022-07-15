import user_page
import time
import sys
import matplotlib.pyplot as plt
import networkx as nx


class VkData:
    # Your VK application ID and access token
    APP_ID = "8204243"
    TOKEN = "vk1.a.EikrQo0QOJ51cZrXXjwhEXwn-aovE2Gbqq80FBLK_2DFV6nqZuoSs5p6FHS2C_89rxfdXSDgLw06y54Av2S2VCcyZAiI262nANr9Q3O9N3ePJF8tQz3BQ9PaBg_m5y0u14Kil4kTGT6kS5_qfQUICp-arLeOFKYqFbot82oM2JGJlMFObA7RpasF9wH__B5Y"

    # VK API version
    VERSION = "5.103"


# Class to store user info
class User:
    def __init__(self, us_info):
        self.id = us_info["id"]
        self.first_name = us_info["first_name"]
        self.last_name = us_info["last_name"]

        if "is_closed" in us_info:
            self.is_closed = us_info["is_closed"]
        else:
            self.is_closed = True

        if "is_deactivated" in us_info:
            self.is_closed = True

        self.domain = us_info["domain"]

    def __str__(self):
        return "{0} {1}\n".format(self.first_name, self.last_name)


# Get list of friends by user_id
def get_friends(vk_api, user_id, fields="domain"):
    friends = vk_api.friends.get(user_id=user_id, fields=fields)

    lst = [User(fr) for fr in friends["items"]]

    # In vk API there is a limit on requests per second.
    # Therefore, we sleep
    time.sleep(1)

    return lst


def main():
    if len(sys.argv) > 1:
        id = sys.argv[1]
    else:
        id = input("Введите id пользователя (цифры): ").lstrip()

    # Authorization
    session = user_page.AuthSession(access_token=VkData.TOKEN)
    vk_api = user_page.API(session, v=VkData.VERSION)

    # If user enters screen_name, we need to get his ID
    id = vk_api.users.get(user_ids=id)[0]["id"]

    # Friends Graph is dictionary
    # Key - friend, graph vertex
    # Value - list of mutual friends, adjacent vertices
    graph = {}

    # Get list of friends for entered ID
    friends = get_friends(vk_api, id)

    # Fill graph
    for friend in friends:
        print('Processing', "\tid: ", friend.id,
              "\tName : ", friend.first_name, friend.last_name)

        # If the profile is not hidden
        if not friend.is_closed:
            # Get friends of friend
            all_friends = get_friends(vk_api, friend.id)

            # Find mutual friends
            mutual = []

            for i in all_friends:
                for j in friends:
                    if i.id == j.id:
                        mutual.append(j)

            # Add value in dictionary
            graph[friend] = mutual
        else:
            graph[friend] = list()

    # Graph visualisation
    g = nx.from_dict_of_lists(graph)

    options = {
        'node_color': 'r',
        'node_size': 100,
        'line_color': 'black',
        'with_labels': True,
        'font_color': 'k',
        'style': 'dotted',
    }

    nx.draw_spring(g, **options)
    plt.show()


if __name__ == '__main__':
    main()
