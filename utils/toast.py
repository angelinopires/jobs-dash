"""
Simple toast notification system for Streamlit.
Displays notifications below the button using Streamlit's built-in components.
"""

import streamlit as st
from typing import Literal

ToastType = Literal["success", "error", "warning", "info"]

def show_toast(message: str, toast_type: ToastType = "info", key: str = None):
    """
    Display a simple toast notification below the button.
    
    Args:
        message: The message to display
        toast_type: Type of toast (success, error, warning, info)
        key: Unique key for the toast (optional)
    """
    
    # Generate unique key if not provided
    if key is None:
        key = f"toast_{hash(message)}_{hash(toast_type)}"
    
    # Store toast in session state
    if 'toasts' not in st.session_state:
        st.session_state.toasts = {}
    
    st.session_state.toasts[key] = {
        "message": message,
        "type": toast_type,
        "timestamp": st.session_state.get('_toast_timestamp', 0) + 1
    }
    st.session_state['_toast_timestamp'] = st.session_state.get('_toast_timestamp', 0) + 1

def display_toasts():
    """Display all active toasts below the button."""
    if 'toasts' not in st.session_state or not st.session_state.toasts:
        return
    
    # Display each toast
    for key, toast in st.session_state.toasts.items():
        if toast["type"] == "success":
            st.success(f"✅ {toast['message']}")
        elif toast["type"] == "error":
            st.error(f"❌ {toast['message']}")
        elif toast["type"] == "warning":
            st.warning(f"⚠️ {toast['message']}")
        else:
            st.info(f"ℹ️ {toast['message']}")

def clear_toasts():
    """Clear all toasts."""
    if 'toasts' in st.session_state:
        st.session_state.toasts.clear()

def remove_toast(key: str):
    """Remove a specific toast by key."""
    if 'toasts' in st.session_state and key in st.session_state.toasts:
        del st.session_state.toasts[key]

# Simple success/error functions for quick use
def success_toast(message: str, key: str = None):
    """Show a success toast."""
    show_toast(message, "success", key)

def error_toast(message: str, key: str = None):
    """Show an error toast."""
    show_toast(message, "error", key)

def warning_toast(message: str, key: str = None):
    """Show a warning toast."""
    show_toast(message, "warning", key)

def info_toast(message: str, key: str = None):
    """Show an info toast."""
    show_toast(message, "info", key)
