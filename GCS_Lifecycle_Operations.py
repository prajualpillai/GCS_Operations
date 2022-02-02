from google.cloud import storage


# Assuming that export command has been already run

class Lifecycle:

    def __init__(self):
        self.client = storage.Client()

    def update_bucket_lifecycle(self, event_to_be_performed, storage_class, bucket_name, bucket_object_filters):
        """
        Update Lifecycle Policy of a bucket
        :params
            bucket_name: (str) - name of the bucket that will be accessed in GCS

            event_to_be_performed: (str) - Determine the type of rule that has to be set up
                    a. set_storage: change in storage class after a set period of time
                    b. delete: delete object after a set period of time

            storage_class: (str) - Type of gcs storage that has to be designated
                    a. STANDARD
                    b. NEARLINE
                    c. COLDLINE
                    d. ARCHIVE

            bucket_object_filters: (dict) - contains the parameters which have to be passed for lifecycle update based on
                                            which the objects are filtered

                age: (int) - period after which the action has to be taken

                created_before: (datetime.date) - apply rule action to items created
                               before this date.

                matches_storage_class: (list) - apply rule action to items which
                                        whose storage class matches this value.

                is_live: (bool) -  if true, apply rule action to non-versioned
                            items, or to items with no newer versions. If false, apply
                            rule action to versioned items with at least one newer
                            version.
                number_of_newer_versions: (int) - apply rule action to versioned
                                            items having N newer versions.

                If any of the values are not present or not required then they can be omitted from parameters

        :returns:
            boolean - True for Success, False otherwise

        """
        try:
            bucket = self.client.bucket(bucket_name)

            if event_to_be_performed is "set_storage":
                bucket.add_lifecycle_set_storage_class_rule(storage_class=storage_class,
                                                            **bucket_object_filters)
            elif event_to_be_performed is "delete":
                bucket.add_lifecycle_delete_rule(**bucket_object_filters)

            bucket.patch()

        except Exception as err:
            print('..exception..', err)
            return False

        return True

    def get_lifecycle(self, bucket_name):
        """
        This function returns the lifecycle currently on the bucket
        :param
            bucket_name: (str) - The bucket that has to be accessed
        :return:
            A list of dictionaries containing the rules applied on the bucket
        """
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        return list(bucket.lifecycle_rules)
