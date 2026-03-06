import React, { useState } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import {
  Box,
  Drawer,
  AppBar,
  Toolbar,
  List,
  Typography,
  Divider,
  IconButton,
  ListItem,
  ListItemButton,
  ListItemIcon,
  ListItemText,
  Chip,
  Avatar,
  Menu,
  MenuItem,
} from '@mui/material';
import {
  Menu as MenuIcon,
  Dashboard as DashboardIcon,
  Science as ScienceIcon,
  People as PeopleIcon,
  Assignment as AssignmentIcon,
  Build as BuildIcon,
  Description as DescriptionIcon,
  HelpOutline as QueryIcon,
  Assessment as ReportsIcon,
  Security as SecurityIcon,
  AccountCircle,
  ExitToApp,
  Settings,
} from '@mui/icons-material';

const drawerWidth = 240;

interface Props {
  children: React.ReactNode;
  handleLogout?: () => void;
  userData?: any;
}

const Layout: React.FC<Props> = ({ children, handleLogout, userData }) => {
  const [mobileOpen, setMobileOpen] = useState(false);
  const [anchorEl, setAnchorEl] = useState<null | HTMLElement>(null);
  const navigate = useNavigate();
  const location = useLocation();

  const handleDrawerToggle = () => {
    setMobileOpen(!mobileOpen);
  };

  const handleProfileMenuOpen = (event: React.MouseEvent<HTMLElement>) => {
    setAnchorEl(event.currentTarget);
  };

  const handleProfileMenuClose = () => {
    setAnchorEl(null);
  };

  const handleLogoutClick = () => {
    handleProfileMenuClose();
    if (handleLogout) {
      handleLogout();
    }
    navigate('/login');
  };

  const menuItems: Array<{ text: string; icon: React.ReactNode; path: string; badge?: number }> = [
    { text: 'Dashboards & Reports', icon: <DashboardIcon />, path: '/' },
    { text: 'Form Builder', icon: <BuildIcon />, path: '/form-builder' },
    { text: 'Saved Forms', icon: <DescriptionIcon />, path: '/saved-forms' },
    { text: 'Studies', icon: <ScienceIcon />, path: '/studies' },
    { text: 'Subjects', icon: <PeopleIcon />, path: '/subjects' },
    { text: 'Queries', icon: <QueryIcon />, path: '/queries' },
    { text: 'Audit Trail', icon: <SecurityIcon />, path: '/audit-trail' },
  ];

  const getPhaseColor = (phase?: string) => {
    switch (phase) {
      case 'START_UP': return 'primary';
      case 'CONDUCT': return 'success';
      case 'CLOSE_OUT': return 'warning';
      default: return 'default';
    }
  };

  const getPhaseLabel = (phase?: string) => {
    return phase ? phase.replace('_', '-') : 'UNKNOWN';
  };

  const getPhaseName = (userData: any) => {
    if (!userData) return 'Guest User';
    
    const phase = userData.phase;
    const username = userData.username;
    
    switch (phase) {
      case 'START_UP':
        return username === 'startup_admin' ? 'START-UP Phase Admin' : 'START-UP Phase User';
      case 'CONDUCT':
        return username === 'conduct_crc' ? 'CONDUCT Phase CRC' : 'CONDUCT Phase User';
      case 'CLOSE_OUT':
        return username === 'closeout_manager' ? 'CLOSE-OUT Phase Manager' : 'CLOSE-OUT Phase User';
      default:
        return `${userData.firstName} ${userData.lastName}`;
    }
  };

  const drawer = (
    <div>
      <Toolbar>
        <Typography variant="h6" noWrap component="div" sx={{ fontWeight: 'bold' }}>
          EDC System
        </Typography>
        <Chip label="v1.0" size="small" sx={{ ml: 1 }} color="primary" />
      </Toolbar>
      <Divider />
      <List>
        {menuItems.map((item) => (
          <ListItem key={item.text} disablePadding>
            <ListItemButton
              selected={location.pathname === item.path}
              onClick={() => navigate(item.path)}
            >
              <ListItemIcon>{item.icon}</ListItemIcon>
              <ListItemText primary={item.text} />
              {item.badge && (
                <Chip label={item.badge} size="small" color="error" />
              )}
            </ListItemButton>
          </ListItem>
        ))}
      </List>
      <Divider />
      <Box sx={{ p: 2 }}>
        <Typography variant="caption" color="text.secondary">
          Study Phase
        </Typography>
        <Chip 
          label={getPhaseLabel(userData?.phase)} 
          color={getPhaseColor(userData?.phase) as any} 
          sx={{ mt: 1, width: '100%' }} 
        />
        <Typography variant="caption" color="text.secondary" sx={{ mt: 2, display: 'block' }}>
          Current User
        </Typography>
        <Typography variant="body2" sx={{ mt: 0.5 }}>
          {getPhaseName(userData)}
        </Typography>
        <Typography variant="caption" color="text.secondary">
          {userData?.role || 'No Role'}
        </Typography>
        <Divider sx={{ my: 2 }} />
        <Typography variant="caption" color="text.secondary" sx={{ display: 'block' }}>
          Compliance
        </Typography>
        <Chip label="21 CFR Part 11" size="small" sx={{ mt: 1 }} />
      </Box>
    </div>
  );

  return (
    <Box sx={{ display: 'flex' }}>
      <AppBar
        position="fixed"
        sx={{
          width: { sm: `calc(100% - ${drawerWidth}px)` },
          ml: { sm: `${drawerWidth}px` },
        }}
      >
        <Toolbar>
          <IconButton
            color="inherit"
            aria-label="open drawer"
            edge="start"
            onClick={handleDrawerToggle}
            sx={{ mr: 2, display: { sm: 'none' } }}
          >
            <MenuIcon />
          </IconButton>
          <Typography variant="h6" noWrap component="div" sx={{ flexGrow: 1 }}>
            EDC Clinical Trials System - {userData?.phase ? getPhaseLabel(userData.phase) + ' Phase' : 'Loading...'}
          </Typography>
          <Chip
            label={userData ? `${getPhaseName(userData)} (${userData.role})` : 'Loading...'}
            avatar={<Avatar>{userData ? (userData.phase ? userData.phase[0] : userData.firstName[0]) : '?'}</Avatar>}
            variant="outlined"
            sx={{ color: 'white', borderColor: 'white', mr: 2 }}
          />
          <IconButton
            size="large"
            edge="end"
            aria-label="account of current user"
            aria-haspopup="true"
            onClick={handleProfileMenuOpen}
            color="inherit"
          >
            <AccountCircle />
          </IconButton>
          <Menu
            anchorEl={anchorEl}
            open={Boolean(anchorEl)}
            onClose={handleProfileMenuClose}
          >
            <MenuItem onClick={handleProfileMenuClose}>
              <ListItemIcon>
                <AccountCircle fontSize="small" />
              </ListItemIcon>
              Profile
            </MenuItem>
            <MenuItem onClick={handleProfileMenuClose}>
              <ListItemIcon>
                <Settings fontSize="small" />
              </ListItemIcon>
              Settings
            </MenuItem>
            <Divider />
            <MenuItem onClick={handleLogoutClick}>
              <ListItemIcon>
                <ExitToApp fontSize="small" />
              </ListItemIcon>
              Logout
            </MenuItem>
          </Menu>
        </Toolbar>
      </AppBar>
      <Box
        component="nav"
        sx={{ width: { sm: drawerWidth }, flexShrink: { sm: 0 } }}
      >
        <Drawer
          variant="temporary"
          open={mobileOpen}
          onClose={handleDrawerToggle}
          ModalProps={{
            keepMounted: true,
          }}
          sx={{
            display: { xs: 'block', sm: 'none' },
            '& .MuiDrawer-paper': {
              boxSizing: 'border-box',
              width: drawerWidth,
            },
          }}
        >
          {drawer}
        </Drawer>
        <Drawer
          variant="permanent"
          sx={{
            display: { xs: 'none', sm: 'block' },
            '& .MuiDrawer-paper': {
              boxSizing: 'border-box',
              width: drawerWidth,
            },
          }}
          open
        >
          {drawer}
        </Drawer>
      </Box>
      <Box
        component="main"
        sx={{
          flexGrow: 1,
          p: 3,
          width: { sm: `calc(100% - ${drawerWidth}px)` },
          minHeight: '100vh',
          backgroundColor: 'background.default',
        }}
      >
        <Toolbar />
        {children}
      </Box>
    </Box>
  );
};

export default Layout;