import json
import os
import logging

logger = logging.getLogger(__name__)

class SettingsManager:
    def __init__(self, settings_file_path='settings.json'):
        self.settings_file = settings_file_path
        self.settings = {}
        self.default_settings = {
            "opensearch": {
                "host": "https://192.168.3.66:9200",
                "user": "admin",
                "password": "123@QWE#asd", # Use a placeholder default
                "index_name": "medical_records"
            },
            "webdav": {
                "ip": "",
                "port": 6000,
                "user": "",
                "password": "",
                "directory": "",
                "enabled": False
            },
            "localfile": {
                "pdf_directory": "pdf_files" # Use a placeholder default
            }
        }
        self.load_settings() # Load settings on initialization

    def load_settings(self):
        """Loads all settings from the JSON file."""
        if os.path.exists(self.settings_file):
            try:
                with open(self.settings_file, 'r', encoding='utf-8') as f:
                    self.settings = json.load(f)
                logger.info(f"Settings loaded successfully from {self.settings_file}")

                # Validate and merge with defaults to handle missing keys
                self._validate_and_merge_defaults(self.settings, self.default_settings)

                # Specific type handling for webdav_enabled
                webdav_enabled = self.settings.get('webdav', {}).get('enabled', self.default_settings['webdav']['enabled'])
                if isinstance(webdav_enabled, str):
                     self.settings['webdav']['enabled'] = webdav_enabled.lower() == 'true'
                else:
                     self.settings['webdav']['enabled'] = bool(webdav_enabled)

            except (IOError, json.JSONDecodeError) as e:
                logger.error(f"Error loading or parsing settings from {self.settings_file}: {e}")
                self.settings = self.default_settings.copy() # Use default on error
                logger.info("Using default settings due to load error.")

        else:
            self.settings = self.default_settings.copy() # Use default if file doesn't exist
            logger.info(f"{self.settings_file} not found, using default settings.")
            # Optionally save the default settings structure to the file
            # self.save_settings()


    def _validate_and_merge_defaults(self, current_dict, default_dict):
        """Recursively merges default values for missing keys."""
        for key, default_value in default_dict.items():
            if key not in current_dict:
                current_dict[key] = default_value
                logger.warning(f"Added missing setting '{key}' with default value.")
            elif isinstance(default_value, dict) and isinstance(current_dict[key], dict):
                # Recurse for nested dictionaries
                self._validate_and_merge_defaults(current_dict[key], default_value)



    def save_settings(self):
        """Saves current settings to the JSON file."""
        try:
            # Ensure the webdav_enabled is stored as a boolean before saving
            if 'webdav' in self.settings and 'enabled' in self.settings['webdav']:
                 self.settings['webdav']['enabled'] = bool(self.settings['webdav']['enabled'])

            with open(self.settings_file, 'w', encoding='utf-8') as f:
                json.dump(self.settings, f, indent=4, ensure_ascii=False)
            logger.info(f"Settings saved successfully to {self.settings_file}")
        except IOError as e:
            logger.error(f"Error saving settings to {self.settings_file}: {e}")

    def get_all_settings(self):
        """Returns a copy of all settings."""
        return self.settings.copy()

    def get_webdav_settings(self):
        """Returns a copy of the WebDAV settings section."""
        return self.settings.get('webdav', {}).copy()

    def update_webdav_settings(self, webdav_data):
        """Updates the webdav section of the settings and saves."""
        if not isinstance(webdav_data, dict):
            logger.warning("Invalid data format for updating WebDAV settings.")
            return False # Indicate failure

        # Ensure the webdav section exists
        if 'webdav' not in self.settings or not isinstance(self.settings['webdav'], dict):
            self.settings['webdav'] = {}
            logger.warning("WebDAV section missing in settings, initializing.")

        # Update only the keys that are allowed in the default webdav structure
        # This prevents adding arbitrary keys via the API
        for key in self.default_settings['webdav'].keys():
             if key in webdav_data:
                  # Handle boolean conversion for enabled specifically if present in data
                  if key == 'enabled':
                       enabled_value = webdav_data[key]
                       if isinstance(enabled_value, str):
                            self.settings['webdav'][key] = enabled_value.lower() == 'true'
                       else:
                            self.settings['webdav'][key] = bool(enabled_value)
                  else:
                       self.settings['webdav'][key] = webdav_data[key]
        
        # Save the updated settings
        self.save_settings()
        return True # Indicate success