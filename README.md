# 🤖 Live Job Search Agent

A powerful, real-time job scraping dashboard built with **Streamlit** and **jobspy**. This application allows users to search for job positions across multiple job boards simultaneously, with an intuitive web interface and interactive results display.

## 🛠️ Technology Stack

### **Frontend & Framework**
- **Streamlit**: Web application framework (like React but for Python)
- **Pandas**: Data manipulation and analysis
- **HTML/Markdown**: Rich text rendering

### **Backend & Scraping**
- **jobspy**: Multi-site job scraping library
- **Python 3.8+**: Core programming language
- **Session State**: Persistent data management

## 📦 Installation

### **Prerequisites**
- Python 3.8 or higher
- pip (Python package manager)

### **🚀 Quick Setup (Recommended)**

**Option 1: Automated Setup**
```bash
git clone <repository-url>
cd jobs-dashboard
./setup.sh
source venv/bin/activate
streamlit run dashboard.py
```

**Option 2: Manual Setup**
```bash
git clone <repository-url>
cd jobs-dashboard
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
streamlit run dashboard.py
```

### **🎯 Virtual Environment Best Practices**

**Why Virtual Environments?**
- **Isolation**: Each project has its own dependencies
- **Version Control**: Avoid conflicts between different projects
- **Clean Environment**: No system-wide package pollution
- **Reproducibility**: Same environment across different machines

**Essential Commands:**
```bash
# Activate environment
source venv/bin/activate

# Deactivate environment
deactivate

# Check active environment
which python  # Should show venv/bin/python

# Update dependencies
pip install --upgrade -r requirements.txt
```

### **Access the Dashboard**
- Open your browser to `http://localhost:8501`
- The dashboard will load automatically

## 🔧 Customization

### **Adding New Job Sites**
1. Check `jobspy` documentation for supported sites
2. Add site name to `site_options` list
3. Update default selection if needed

### **Styling Changes**
- Modify `st.set_page_config()` for page appearance
- Update emojis and text for different themes
- Adjust column layouts with `st.columns()`

## 🚨 Troubleshooting

### **Common Issues**

1. **"No jobs found"**
   - Check your search terms and location
   - Verify selected sites are available
   - Try different country settings

2. **Scraping errors**
   - Ensure stable internet connection
   - Some sites may block automated requests
   - Try reducing results count

3. **Missing dependencies**
   ```bash
   pip install --upgrade streamlit pandas jobspy
   ```

4. **Port conflicts**
   ```bash
   streamlit run dashboard.py --server.port 8502
   ```

### **Performance Tips**
- Start with fewer results (5-10) for faster scraping
- Use specific search terms for better results
- Select fewer sites for quicker responses

## 📊 Data Structure

### **Job Object Fields**
```python
{
    'title': 'Software Engineer',
    'company': 'Tech Corp',
    'location': 'San Francisco, CA',
    'date_posted': '2024-01-15',
    'description': 'Full job description...',
    'salary': '$100k - $150k',
    'job_url': 'https://example.com/job',
    'source': 'linkedin'
}
```

### **Supported Job Sites**
- **LinkedIn**: Professional networking platform
- **Indeed**: Global job search engine
- **Glassdoor**: Company reviews and job listings
- **ZipRecruiter**: AI-powered job matching

## 🤝 Contributing

### **Development Setup**
1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## 📝 License

This project is open source and available under the [MIT License](LICENSE).

## 🙏 Acknowledgments

- **jobspy** library for multi-site scraping capabilities
- **Streamlit** team for the amazing web framework
- **Pandas** community for data manipulation tools

---

**Happy job hunting! 🎯**

*Built with ❤️ and Python*
