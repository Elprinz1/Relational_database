import streamlit as st

from hw1 import *
from user_definition import *


if 'dir' not in st.session_state:
    st.session_state.dir = None

folder_path = st.text_input('Absoulte path of the directory\
                             for input data:')
if folder_path:
    st.write('You entered: ', folder_path)
    st.session_state.dir = folder_path
    drop_tables(user, host, dbname)
    create_tables(user, host, dbname)
    copy_data(user, host, dbname, st.session_state.dir)
