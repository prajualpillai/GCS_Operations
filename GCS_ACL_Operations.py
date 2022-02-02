from google.cloud import storage


class ACLOperations:

    def __init__(self):
        self.client = storage.Client()

    def update_bucket_acl(self, acls, bucket_name):
        """
        This function will update the ACL policies on the GCS bucket
        The GCS bucket requires following values:
            access_id: (str) - (user)name of the id, i.e domain, group, user, etc.
            role: (str) - role to be given, can be one of these values:
                a. READER
                b. WRITER
                c. OWNER
            type_of_access_id: (str) -
                a. allUsers               #Public
                b. allAuthenticatedUsers  #Public
                c. domain   #Not valid right now
                d. group
                e. user
        A mapping has been assumed and imported from the constants file, which can be changed
        or updated with further update.

        :param:
            acls: (dictionary)- The ACLs for GCS bucket, will have the following values:
                access_id
                type_of_access_id
                role
            bucket_name: (str) - name of the bucket to be accessed in GCS
        :returns:
                status: boolean - True for Success, False otherwise
        """
        rules_to_be_applied = []
        try:

            bucket = self.client.bucket(bucket_name)
            if bucket.exists():
                # Disable uniform bucket level access; required to update acl
                bucket.iam_configuration.uniform_bucket_level_access_enabled = False
                # Update the same on the bucket
                bucket.patch()
                bucket_acl = bucket.acl

                try:
                    type_of_access_id = acls.get('type_of_access_id')
                    access_id = acls.get('access_id')
                    role = acls.get('role')
                    if role:
                        rules_to_be_applied.append({'entity': f"{type_of_access_id}-{access_id}",
                                                    'role': role})
                    else:
                        # Apply IAM
                        bucket.iam_configuration.uniform_bucket_level_access_enabled = True
                        bucket.patch()

                except KeyError as err:
                    print("..exception..", err)
                    return False

                bucket_acl.save(acl=rules_to_be_applied)
                return True
        except Exception as err:
            print('..exception..', err)
            return False
        return False

    def get_acl(self, bucket_name):
        """
        This function will access the acl currently set on the bucket
        :param
            bucket_name: (str) -  name of the bucket to be accessed in GCS
        :return:
            status: boolean - True for Success, False otherwise
        """

        try:
            bucket = self.client.bucket(bucket_name)
            acl = bucket.acl
            for entry in acl:
                print("{}: {}".format(entry["role"], entry["entity"]))
            return True
        except Exception as err:
            print("...exception... ", err)
            return False
