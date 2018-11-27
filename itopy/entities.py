import json
from collections import namedtuple


class BaseEntity(object):

    OBJECT_CLASS = "Fake"

    def __init__(self, client):
        self.id = None
        self.client = client
        self.me = None

    def find(self, where):
        return self.client.get(OBJECT_CLASS, "SELECT {} WHERE {}".format(OBJECT_CLASS, where))

    def find_by_name(self, name, active=True):
        if not hasattr(self, 'klass'):
            raise ValueError("find_by_name cannot be called on {} object".format(BaseEntity.OBJECT_CLASS))

        query = "SELECT {}  WHERE name = '{}'".format(self.klass, name)
        if not active:
            query += " AND status = 0 "

        data = self.client.get(self.klass, query)

        return self.deserialize(data)

    def find_by_id(self, id, active=True):
        if not hasattr(self, 'klass'):
            raise ValueError("find_by_name cannot be called on {} object".format(BaseEntity.OBJECT_CLASS))

        query = "SELECT {} WHERE id = '{}'".format(self.klass, id)
        if not active:
            query += " AND status = 0 "

        data = self.client.get(self.klass, query)

        return self.deserialize(data)

    def delete(self):
        raise NotImplementedError

    def update(self):
        raise NotImplementedError

    def deserialize(self, data):
        return Utils.deserialize(self.klass, data, self.client)


class Organization(BaseEntity):

    OBJECT_CLASS = "Organization"

    def __init__(self, client):
        super(Organization, self).__init__(client)
        self.klass = Organization.OBJECT_CLASS

    def find_user_requests(self, active=True):
        if not self.me:
            raise ValueError("Method must be called on valid Organization")

        ur = UserRequest(self.client)
        return ur.find_by_organization_id(self.id, active=active)


class UserRequest(BaseEntity):
    
    OBJECT_CLASS = "UserRequest"

    def __init__(self, client):
        super(UserRequest, self).__init__(client)
        self.klass = UserRequest.OBJECT_CLASS

    def find_by_organization_id(self, id, active=True, last_update=None):
        if not hasattr(self, 'klass'):
            raise ValueError("find_by_organization_id cannot be called on {} object".format(BaseEntity.OBJECT_CLASS))

        query = "SELECT {} WHERE org_id = '{}'".format(self.klass, int(id))
        if not active:
            query += " AND status IN ('rejected', 'resolved', 'closed')"
        else:
            query += " AND status NOT IN ('rejected', 'resolved', 'closed')"

        data = self.client.get(self.klass, query)

        return self.deserialize(data)

    def find_by_assignee(self, name, org_id=None, active=True, last_update=None):
        if not hasattr(self, 'klass'):
            raise ValueError("find_by_assegnee cannot be called on {} object".format(BaseEntity.OBJECT_CLASS))

        query = "SELECT {} WHERE agent_id_friendlyname = '{}'".format(self.klass, name)
        if not active:
            query += " AND status IN ('rejected', 'resolved', 'closed') "
        else:
            query += " AND status NOT IN ('rejected', 'resolved', 'closed') "

        if org_id:
            query += " AND org_id = '{}'".format(org_id)

        if last_update:
            assert type(last_update) is datetime
            query += " AND last_update >= {}".format(last_update)

        data = self.client.get(self.klass, query)

        return self.deserialize(data)

        
        

class Utils:

    @staticmethod
    def deserialize(klass, data, client):
        """ 
            Returns a generic Result response with the following main properties:
            - objects: list of object for the requested entity
            - klass: string with the  object class name
            - message: message answered from iTop server
        """

        struct = dict(message=data.get('message', None), objects=None)

        payload = list()
        objects = data.get('objects')
        if not objects:
            struct['objects'] = list()
        else:
            for k, v in objects.iteritems():
                klass, id = k.split('::')
                v['fields']['id'] = id
                obj = globals().get(klass)(client)
                obj.id = id
                obj.me = Utils.convert(klass, v.get('fields'))
                payload.append(obj)
                
        struct['objects'] = payload
        result = namedtuple('Result', sorted(struct))(**struct)
        return result


    @staticmethod
    def convert(name, dictionary):
        for key, value in dictionary.iteritems():
            if isinstance(value, dict):
                dictionary[key] = Utils.convert(name, value)

        return namedtuple(name, dictionary.keys())(**dictionary)

